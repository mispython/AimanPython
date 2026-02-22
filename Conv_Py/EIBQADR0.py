# !/usr/bin/env python3
"""
PROGRAM : EIBQADR0
PURPOSE : PREPARE AND FILTER DEPOSIT, LOAN, AND HIRE PURCHASE DATA FOR PB PREMIUM CLUB PROCESSING.
          PRODUCES ADDR.AUT, ADDR.NEW, ADDR.P50, ADDR.HL, ADDR.HP, AND ADDR.REPTDATE OUTPUT DATASETS.
"""

import duckdb
import polars as pl
import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR        = Path("input")
OUTPUT_DIR       = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input parquet files
PARQUET_MNITB_REPTDATE = INPUT_DIR / "mnitb_reptdate.parquet"
PARQUET_CISC_DEPOSIT   = INPUT_DIR / "cisc_deposit.parquet"
PARQUET_CISS_DEPOSIT   = INPUT_DIR / "ciss_deposit.parquet"
PARQUET_CIS_LOAN       = INPUT_DIR / "cis_loan.parquet"

# Output parquet files (ADDR library)
PARQUET_ADDR_REPTDATE  = OUTPUT_DIR / "addr_reptdate.parquet"
PARQUET_ADDR_AUT       = OUTPUT_DIR / "addr_aut.parquet"
PARQUET_ADDR_NEW       = OUTPUT_DIR / "addr_new.parquet"
PARQUET_ADDR_P50       = OUTPUT_DIR / "addr_p50.parquet"
PARQUET_ADDR_HL        = OUTPUT_DIR / "addr_hl.parquet"
PARQUET_ADDR_HP        = OUTPUT_DIR / "addr_hp.parquet"

# Dynamic input parquet path templates (resolved after REPTDATE is read)
# DP.SAB<YEAR><MON><WK>    -> dp_sab{year}{mon}{wk}.parquet
# DP.CAB<YEAR><MON><WK>    -> dp_cab{year}{mon}{wk}.parquet
# LOAN.LNB<YEAR><MON><WK>  -> loan_lnb{year}{mon}{wk}.parquet
# HP.HP<MON><WK1><YEAR>    -> hp_hp{mon}{wk1}{year}.parquet
DP_SAB_TEMPLATE   = str(INPUT_DIR / "dp_sab{year}{mon}{wk}.parquet")
DP_CAB_TEMPLATE   = str(INPUT_DIR / "dp_cab{year}{mon}{wk}.parquet")
LOAN_LNB_TEMPLATE = str(INPUT_DIR / "loan_lnb{year}{mon}{wk}.parquet")
HP_HP_TEMPLATE    = str(INPUT_DIR / "hp_hp{mon}{wk1}{year}.parquet")


# ---------------------------------------------------------------------------
# Helper: read parquet via DuckDB -> Polars DataFrame
# ---------------------------------------------------------------------------
def read_parquet(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM '{path}'").pl()
    con.close()
    return df


def write_parquet(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path)


# ---------------------------------------------------------------------------
# STEP 1 : DATA ADDR.REPTDATE
#   SET MNITB.REPTDATE;
#   Derive SDD, WK, WK1 based on DAY(REPTDATE)
#   CALL SYMPUT: NOWK, NOWK1, REPTYEAR, REPTDAY, REPTMON, RDATE
# ---------------------------------------------------------------------------
def process_reptdate() -> dict:
    df = read_parquet(PARQUET_MNITB_REPTDATE)

    sdds, wks, wk1s = [], [], []
    for rd in df["REPTDATE"].to_list():
        if not isinstance(rd, datetime.date):
            rd = datetime.date.fromisoformat(str(rd))
        day = rd.day
        if day == 8:
            sdd, wk, wk1 = 1,  "01", "1"
        elif day == 15:
            sdd, wk, wk1 = 9,  "02", "2"
        elif day == 22:
            sdd, wk, wk1 = 16, "03", "3"
        else:
            sdd, wk, wk1 = 23, "04", "4"
        sdds.append(sdd); wks.append(wk); wk1s.append(wk1)

    df = df.with_columns([
        pl.Series("SDD", sdds),
        pl.Series("WK",  wks),
        pl.Series("WK1", wk1s),
    ])

    write_parquet(df, PARQUET_ADDR_REPTDATE)

    # Derive macro-variable equivalents from first row
    row0 = df.row(0, named=True)
    rd   = row0["REPTDATE"]
    if not isinstance(rd, datetime.date):
        rd = datetime.date.fromisoformat(str(rd))

    nowk     = row0["WK"]
    nowk1    = row0["WK1"]
    reptyear = str(rd.year)[-2:]           # YEAR2. -> last 2 digits
    reptday  = str(rd.day).zfill(2)        # Z2.
    reptmon  = str(rd.month).zfill(2)      # Z2.
    # RDATE: PUT(REPTDATE,Z5.) -> SAS date integer (days since 01-JAN-1960), zero-padded 5 chars
    sas_epoch   = datetime.date(1960, 1, 1)
    sas_date_val = (rd - sas_epoch).days
    rdate = str(sas_date_val).zfill(5)

    macros = {
        "NOWK":     nowk,
        "NOWK1":    nowk1,
        "REPTYEAR": reptyear,
        "REPTDAY":  reptday,
        "REPTMON":  reptmon,
        "RDATE":    rdate,
    }
    return macros


# ---------------------------------------------------------------------------
# STEP 2 : DATA CISDP
#   SET CISC.DEPOSIT + CISS.DEPOSIT;
#   IF INDORG='I' AND SECCUST='901';
#   ICNO = NEWIC; if blank -> OLDIC; if still blank -> DELETE
#   IF CUSTNAME blank -> DELETE
#   RENAME CUSTNAME=NAME; DROP BRANCH
#   PROC SORT BY ACCTNO
# ---------------------------------------------------------------------------
def process_cisdp() -> pl.DataFrame:
    df_cisc = read_parquet(PARQUET_CISC_DEPOSIT)
    df_ciss = read_parquet(PARQUET_CISS_DEPOSIT)

    df = pl.concat([df_cisc, df_ciss], how="diagonal_relaxed")

    df = df.filter(
        (pl.col("INDORG").cast(pl.Utf8).str.strip_chars()  == "I") &
        (pl.col("SECCUST").cast(pl.Utf8).str.strip_chars() == "901")
    )

    # ICNO = NEWIC; if blank use OLDIC
    df = df.with_columns(
        pl.when(pl.col("NEWIC").cast(pl.Utf8).str.strip_chars() == "")
          .then(pl.col("OLDIC").cast(pl.Utf8))
          .otherwise(pl.col("NEWIC").cast(pl.Utf8))
          .alias("ICNO")
    )

    # DELETE if ICNO blank or CUSTNAME blank
    df = df.filter(
        (pl.col("ICNO").str.strip_chars()       != "") &
        (pl.col("CUSTNAME").cast(pl.Utf8).str.strip_chars() != "")
    )

    df = df.rename({"CUSTNAME": "NAME"})
    df = df.drop("BRANCH")

    # PROC SORT BY ACCTNO
    df = df.sort("ACCTNO")
    return df


# ---------------------------------------------------------------------------
# STEP 3 : DATA CA / SA / P50 / NEW / AUT
#   Read DP.SAB and DP.CAB dynamic parquet files
# ---------------------------------------------------------------------------
def process_deposits(macros: dict):
    year = macros["REPTYEAR"]
    mon  = macros["REPTMON"]
    wk   = macros["NOWK"]

    sab_path = Path(DP_SAB_TEMPLATE.format(year=year, mon=mon, wk=wk))
    cab_path = Path(DP_CAB_TEMPLATE.format(year=year, mon=mon, wk=wk))

    df_sab = read_parquet(sab_path)
    df_cab = read_parquet(cab_path)

    # DATA CA: IF PRODUCT IN (100,102,150,152,156,157,160,162)
    ca_products = {100, 102, 150, 152, 156, 157, 160, 162}
    ca = df_cab.filter(pl.col("PRODUCT").is_in(ca_products))

    # DATA SA: IF PRODUCT IN (200,202,203,204,212,213,214)
    sa_products = {200, 202, 203, 204, 212, 213, 214}
    sa = df_sab.filter(pl.col("PRODUCT").is_in(sa_products))

    # DATA P50
    p50 = df_sab.filter(
        (pl.col("PRODUCT")  == 203) &
        (pl.col("CURBAL")   > 0) &
        (pl.col("PURPOSE").cast(pl.Utf8).is_in(["1", "2", "H"])) &
        (pl.col("USER3").cast(pl.Utf8).is_in(["2", "4", "7"])) &
        (~pl.col("OPENIND").cast(pl.Utf8).is_in(["B", "C", "P", "Z"]))
    )

    # DATA NEW AUT: combine SA + CA then filter
    sa_ca = pl.concat([sa, ca], how="diagonal_relaxed")
    eligible = sa_ca.filter(
        (~pl.col("OPENIND").cast(pl.Utf8).is_in(["B", "C", "P", "Z"])) &
        (pl.col("PURPOSE").cast(pl.Utf8).is_in(["1", "2", "H"])) &
        (pl.col("CURBAL")   > 0) &
        (pl.col("YTDAVAMT") >= 30000)
    )

    new_df = eligible.filter( pl.col("USER3").cast(pl.Utf8).is_in(["2", "4", "7"]))
    aut_df = eligible.filter(~pl.col("USER3").cast(pl.Utf8).is_in(["2", "4", "7"]))

    # PROC SORT BY ACCTNO
    new_df = new_df.sort("ACCTNO")
    aut_df = aut_df.sort("ACCTNO")
    p50    = p50.sort("ACCTNO")

    return ca, sa, p50, new_df, aut_df


# ---------------------------------------------------------------------------
# STEP 4 : Merge deposits with CISDP, assign LID, deduplicate by ICNO
# ---------------------------------------------------------------------------
def merge_and_dedup(
    deposit_df: pl.DataFrame,
    cisdp: pl.DataFrame,
    lid_val: str
) -> pl.DataFrame:
    """
    MERGE deposit(IN=A) CISDP(IN=B); BY ACCTNO;
    IF A AND B;
    LID = lid_val;
    PROC SORT NODUPKEY BY ICNO;
    """
    merged = deposit_df.join(cisdp, on="ACCTNO", how="inner")
    merged = merged.with_columns(pl.lit(lid_val).alias("LID"))
    # NODUPKEY BY ICNO: keep first occurrence per ICNO (already sorted by ACCTNO)
    merged = merged.unique(subset=["ICNO"], keep="first")
    return merged


# ---------------------------------------------------------------------------
# STEP 5 : DATA CISLN
#   SET CIS.LOAN;
#   IF INDORG='I' AND SECCUST='901';
#   ICNO = NEWIC; if blank -> OLDIC; if still blank -> DELETE
#   IF CUSTNAME blank -> DELETE
#   RENAME CUSTNAME=NAME; DROP BRANCH
#   PROC SORT BY ACCTNO
# ---------------------------------------------------------------------------
def process_cisln() -> pl.DataFrame:
    df = read_parquet(PARQUET_CIS_LOAN)

    df = df.filter(
        (pl.col("INDORG").cast(pl.Utf8).str.strip_chars()  == "I") &
        (pl.col("SECCUST").cast(pl.Utf8).str.strip_chars() == "901")
    )

    df = df.with_columns(
        pl.when(pl.col("NEWIC").cast(pl.Utf8).str.strip_chars() == "")
          .then(pl.col("OLDIC").cast(pl.Utf8))
          .otherwise(pl.col("NEWIC").cast(pl.Utf8))
          .alias("ICNO")
    )

    df = df.filter(
        (pl.col("ICNO").str.strip_chars()       != "") &
        (pl.col("CUSTNAME").cast(pl.Utf8).str.strip_chars() != "")
    )

    df = df.rename({"CUSTNAME": "NAME"})
    df = df.drop("BRANCH")

    # PROC SORT BY ACCTNO
    df = df.sort("ACCTNO")
    return df


# ---------------------------------------------------------------------------
# STEP 6 : DATA HL (Housing Loans)
#   SET LOAN.LNB<YEAR><MON><WK>
#   Filter and merge with CISLN; LID='L4'; NODUPKEY BY ICNO
# ---------------------------------------------------------------------------
def process_hl(macros: dict, cisln: pl.DataFrame) -> pl.DataFrame:
    year = macros["REPTYEAR"]
    mon  = macros["REPTMON"]
    wk   = macros["NOWK"]

    lnb_path = Path(LOAN_LNB_TEMPLATE.format(year=year, mon=mon, wk=wk))
    df = read_parquet(lnb_path)

    # IF ACCTNO = 2023686909 THEN DELETE
    df = df.filter(pl.col("ACCTNO") != 2023686909)

    hl_loantypes = {
        110, 111, 113, 115, 116,
        204, 205, 214, 215, 225, 226, 228, 231, 233, 234,
        235, 236, 238, 240, 241, 242
    }

    df = df.filter(
        (pl.col("PAIDIND").cast(pl.Utf8).str.strip_chars() != "P") &
        (pl.col("APVLIMIT") >= 150000) &
        (pl.col("LOANTYPE").is_in(hl_loantypes)) &
        (pl.col("UNDRAWN")  == 0) &
        (pl.col("CURBAL")   > 0) &
        (pl.col("DPASSDUE") < 62)
    )

    # RENAME SECTOR=SECTORX
    df = df.rename({"SECTOR": "SECTORX"})

    # PROC SORT BY ACCTNO
    df = df.sort("ACCTNO")

    # MERGE CISLN(IN=A) HL(IN=B); BY ACCTNO; IF A AND B; LID='L4'
    merged = cisln.join(df, on="ACCTNO", how="inner")
    merged = merged.with_columns(pl.lit("L4").alias("LID"))

    # PROC SORT DATA=HL OUT=HL NODUPKEY; BY ICNO
    merged = merged.unique(subset=["ICNO"], keep="first")
    return merged


# ---------------------------------------------------------------------------
# STEP 7 : DATA HP (Hire Purchase)
#   SET HP.HP<MON><WK1><YEAR>
#   Filter and merge with CISLN; LID='L5'; NODUPKEY BY ICNO
# ---------------------------------------------------------------------------
def process_hp(macros: dict, cisln: pl.DataFrame) -> pl.DataFrame:
    year      = macros["REPTYEAR"]
    mon       = macros["REPTMON"]
    wk1       = macros["NOWK1"]
    rdate_val = int(macros["RDATE"])

    hp_path = Path(HP_HP_TEMPLATE.format(mon=mon, wk1=wk1, year=year))
    df = read_parquet(hp_path)

    # IF ACCTNO = 2023686909 THEN DELETE
    df = df.filter(pl.col("ACCTNO") != 2023686909)

    # RUNDT = &RDATE
    # TMLATE = SUBSTR(TIMELATE,5,3)  (SAS 1-based pos 5 = Python 0-based index 4)
    # CHKDT  = RUNDT - 365
    chkdt = rdate_val - 365

    df = df.with_columns([
        pl.lit(rdate_val).alias("RUNDT"),
        pl.col("TIMELATE").cast(pl.Utf8).str.slice(4, 3).alias("TMLATE"),
        pl.lit(chkdt).alias("CHKDT"),
    ])

    # IF TMLATE='000'
    df = df.filter(pl.col("TMLATE").str.strip_chars() == "000")

    hp_products = {128, 380, 700}

    df = df.filter(
        (pl.col("PAIDIND").cast(pl.Utf8).str.strip_chars() != "P") &
        (pl.col("PRODUCT").is_in(hp_products)) &
        (pl.col("CURBAL")  != 0) &
        (pl.col("ISSDTE")  < chkdt) &
        (pl.col("ORGTYPE").cast(pl.Utf8).str.strip_chars() == "N") &
        (pl.col("NETPROC") >= 70000) &
        (pl.col("NEWSEC").cast(pl.Utf8).str.strip_chars().is_in(["N", "R"]))
    )

    # PROC SORT BY ACCTNO
    df = df.sort("ACCTNO")

    # MERGE CISLN(IN=A) HP(IN=B); BY ACCTNO; IF A AND B;
    merged = cisln.join(df, on="ACCTNO", how="inner")
    merged = merged.with_columns(pl.lit("L5").alias("LID"))

    # RENAME SECTOR=SECTORX
    if "SECTOR" in merged.columns:
        merged = merged.rename({"SECTOR": "SECTORX"})

    # PROC SORT DATA=HP OUT=HP NODUPKEY; BY ICNO
    merged = merged.unique(subset=["ICNO"], keep="first")
    return merged


# ---------------------------------------------------------------------------
# STEP 8 : DATA PBA — combine all, deduplicate by ICNO keeping lowest LID,
#           split into ADDR.AUT/NEW/P50/HL/HP
# ---------------------------------------------------------------------------
def process_pba(
    new_df: pl.DataFrame,
    aut_df: pl.DataFrame,
    p50_df: pl.DataFrame,
    hl_df:  pl.DataFrame,
    hp_df:  pl.DataFrame,
) -> None:
    # DATA PBA; SET NEW AUT P50 HL HP;
    pba = pl.concat([new_df, aut_df, p50_df, hl_df, hp_df], how="diagonal_relaxed")

    # PROC SORT DATA=PBA; BY ICNO LID;
    pba = pba.sort(["ICNO", "LID"])

    # PROC SORT DATA=PBA OUT=PBA NODUPKEY; BY ICNO;
    pba = pba.unique(subset=["ICNO"], keep="first")

    # Split back into LID-based output datasets
    addr_aut = pba.filter(pl.col("LID") == "L1")
    addr_new = pba.filter(pl.col("LID") == "L2")
    addr_p50 = pba.filter(pl.col("LID") == "L3")
    addr_hl  = pba.filter(pl.col("LID") == "L4")
    addr_hp  = pba.filter(pl.col("LID") == "L5")

    write_parquet(addr_aut, PARQUET_ADDR_AUT)
    write_parquet(addr_new, PARQUET_ADDR_NEW)
    write_parquet(addr_p50, PARQUET_ADDR_P50)
    write_parquet(addr_hl,  PARQUET_ADDR_HL)
    write_parquet(addr_hp,  PARQUET_ADDR_HP)

    print(f"ADDR.AUT written -> {PARQUET_ADDR_AUT}  ({addr_aut.height} rows)")
    print(f"ADDR.NEW written -> {PARQUET_ADDR_NEW}  ({addr_new.height} rows)")
    print(f"ADDR.P50 written -> {PARQUET_ADDR_P50}  ({addr_p50.height} rows)")
    print(f"ADDR.HL  written -> {PARQUET_ADDR_HL}   ({addr_hl.height}  rows)")
    print(f"ADDR.HP  written -> {PARQUET_ADDR_HP}   ({addr_hp.height}  rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # STEP 1: Process REPTDATE and derive macro variables
    macros = process_reptdate()
    print(f"Macro variables resolved: {macros}")

    # STEP 2: Build CISDP (deposit CIS reference, merged from CISC + CISS)
    cisdp = process_cisdp()

    # STEP 3: Build CA, SA, P50, NEW, AUT from balance files
    _ca, _sa, p50_raw, new_raw, aut_raw = process_deposits(macros)

    # STEP 4: Merge deposit sets with CISDP; assign LID; deduplicate by ICNO
    new_df = merge_and_dedup(new_raw, cisdp, "L2")
    aut_df = merge_and_dedup(aut_raw, cisdp, "L1")
    p50_df = merge_and_dedup(p50_raw, cisdp, "L3")

    # STEP 5: Build CISLN (loan CIS reference)
    cisln = process_cisln()

    # STEP 6: Build HL (Housing Loans)
    hl_df = process_hl(macros, cisln)

    # STEP 7: Build HP (Hire Purchase)
    hp_df = process_hp(macros, cisln)

    # STEP 8: Combine PBA, deduplicate, and write ADDR output datasets
    process_pba(new_df, aut_df, p50_df, hl_df, hp_df)

    print("EIBQADR0 processing complete.")


if __name__ == "__main__":
    main()
