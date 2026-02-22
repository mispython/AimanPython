# !/usr/bin/env python3
"""
PROGRAM : EIBQCARD
PURPOSE : PRODUCE BATCHCARD FILE FOR INTRADAY POSTING.
          READS ADDR.AUT, MAPS PRODUCT TO DPFMT CODE, DERIVES USRCD FROM USER3, AND WRITES FIXED-WIDTH
            CARD RECORDS TO OUTPUT FILE (CARD).
NOTE    : THIS STEP WAS COMMENTED OUT IN JCL EIBQADRR.
          OUTPUT DSN: SAP.PBB.PBPREM.TEXT
          DCB: RECFM=FB, LRECL=80, BLKSIZE=25600
"""

import duckdb
import polars as pl
from pathlib import Path

# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------
INPUT_DIR  = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_ADDR_AUT = INPUT_DIR / "addr_aut.parquet"

# //CARD DD DSN=SAP.PBB.PBPREM.TEXT  (RECFM=FB, LRECL=80)
OUTPUT_FILE = OUTPUT_DIR / "EIBQCARD_card.txt"


# ---------------------------------------------------------------------------
# PROC FORMAT: DPFMT
# Maps PRODUCT code -> 3-char deposit format code string
# ---------------------------------------------------------------------------
DPFMT_MAP = {
    200: "001",
    202: "002",
    203: "003",
    212: "004",
    150: "005",
    152: "005",
    204: "006",
    213: "007",
    214: "008",
}


def fmt_dpfmt(product) -> str:
    try:
        return DPFMT_MAP.get(int(product), "   ")
    except (ValueError, TypeError):
        return "   "


# ---------------------------------------------------------------------------
# Helper: read parquet via DuckDB -> Polars DataFrame
# ---------------------------------------------------------------------------
def read_parquet(path: Path) -> pl.DataFrame:
    con = duckdb.connect()
    df  = con.execute(f"SELECT * FROM '{path}'").pl()
    con.close()
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # DATA SAVING; SET ADDR.AUT;
    df = read_parquet(PARQUET_ADDR_AUT)

    # LENGTH USRCD 2.; USRCD = 2;  (default)
    # FORMAT BRANCH Z3. ACCTNO $10.;
    # IF PRODUCT IN (100,102,150,152,156,157,160,162,
    #                200,202,203,204,212,213,214);
    valid_products = {
        100, 102, 150, 152, 156, 157, 160, 162,
        200, 202, 203, 204, 212, 213, 214
    }
    df = df.filter(pl.col("PRODUCT").is_in(valid_products))

    # USRCD default = 2
    df = df.with_columns(pl.lit(2).cast(pl.Int16).alias("USRCD"))

    # IF USER3='3' THEN USRCD=4;
    # IF USER3='6' THEN USRCD=7;
    df = df.with_columns(
        pl.when(pl.col("USER3").cast(pl.Utf8).str.strip_chars() == "3")
          .then(pl.lit(4).cast(pl.Int16))
          .when(pl.col("USER3").cast(pl.Utf8).str.strip_chars() == "6")
          .then(pl.lit(7).cast(pl.Int16))
          .otherwise(pl.col("USRCD"))
          .alias("USRCD")
    )

    # PROD = PUT(PRODUCT,DPFMT.)
    prod_values = [fmt_dpfmt(v) for v in df["PRODUCT"].to_list()]
    df = df.with_columns(pl.Series("PROD", prod_values))

    # PROC SORT BY ACCTNO
    df = df.sort("ACCTNO")

    # ---------------------------------------------------------------------------
    # DATA _NULL_; SET SAVING; FILE CARD;
    # PUT @1 'D033' BRANCH @8 ACCTNO @18 '380' +2 'USRCD3' +2 '='USRCD;
    #
    # SAS column positions (1-based):
    #   @1  : literal 'D033'                    -> cols 1-4    (0-based 0-3)
    #   BRANCH (FORMAT Z3.)                      -> cols 5-7    (0-based 4-6)
    #   @8  : ACCTNO ($10.)                      -> cols 8-17   (0-based 7-16)
    #   @18 : literal '380'                     -> cols 18-20  (0-based 17-19)
    #   +2  : skip 2 columns                    -> cols 21-22  (0-based 20-21)
    #   literal 'USRCD3'                         -> cols 23-28  (0-based 22-27)
    #   +2  : skip 2 columns                    -> cols 29-30  (0-based 28-29)
    #   '=' + USRCD (numeric, right-justified)   -> cols 31+    (0-based 30+)
    #
    # LRECL=80: each record is padded/truncated to 80 characters.
    # ---------------------------------------------------------------------------
    records = []
    for row in df.iter_rows(named=True):
        branch_str = str(int(row["BRANCH"])).zfill(3)          # FORMAT BRANCH Z3.
        acctno_str = str(row["ACCTNO"]).ljust(10)[:10]         # FORMAT ACCTNO $10.
        usrcd_str  = str(int(row["USRCD"]))                    # numeric USRCD

        # Build record positionally (0-based indexing, LRECL=80)
        rec = [" "] * 80

        # @1  'D033'  -> positions 0-3
        for i, ch in enumerate("D033"):
            rec[0 + i] = ch

        # BRANCH (Z3.) -> positions 4-6  (immediately after 'D033', no gap)
        for i, ch in enumerate(branch_str):
            rec[4 + i] = ch

        # @8  ACCTNO ($10.) -> positions 7-16
        for i, ch in enumerate(acctno_str):
            rec[7 + i] = ch

        # @18 '380'   -> positions 17-19
        for i, ch in enumerate("380"):
            rec[17 + i] = ch

        # +2 skip -> positions 20-21 remain spaces

        # 'USRCD3'    -> positions 22-27
        for i, ch in enumerate("USRCD3"):
            rec[22 + i] = ch

        # +2 skip -> positions 28-29 remain spaces

        # '=' + USRCD -> positions 30+
        eq_usrcd = "=" + usrcd_str
        for i, ch in enumerate(eq_usrcd):
            if 30 + i < 80:
                rec[30 + i] = ch

        records.append("".join(rec))

    # Write output card file (RECFM=FB, LRECL=80)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(record + "\n")

    print(f"Card file written to: {OUTPUT_FILE}  ({len(records)} records)")


if __name__ == "__main__":
    main()
