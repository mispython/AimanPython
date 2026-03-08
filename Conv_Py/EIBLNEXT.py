#!/usr/bin/env python3
"""
Program  : EIBLNEXT.py
Purpose  : Route loan datasets into LOAN (conventional) and ILOAN (Islamic)
           libraries based on COSTCTR range (3000-4999 = Islamic/ILOAN).

           Takes all working datasets produced by EIBLNOTE.py and:
             1. Splits LNNOTE    -> LOAN.LNNOTE  / ILOAN.LNNOTE
                       LNNOTE(REVERSED='Y') -> LOAN.RVNOTE / ILOAN.RVNOTE
             2. Splits LNACCT   -> LOAN.LNACCT  / ILOAN.LNACCT
             3. Splits LNACC4   -> LOAN.LNACC4  / ILOAN.LNACC4
             4. Filters LIAB    -> LOAN.LIAB    / ILOAN.LIAB
                (inner-join against note keys from routed LNNOTE)
             5. Filters PEND    -> LOAN.PEND    / ILOAN.PEND
                (inner-join against note keys from routed LNNOTE)
             6. Filters LNCOMM  -> LOAN.LNCOMM  / ILOAN.LNCOMM
                (inner-join against COMMNO keys, with ACCTNO range passthrough)
             7. Filters NAME8   -> LOAN.NAME8   / ILOAN.NAME8
                (inner-join against ACCTNO keys from routed LNNOTE)
             8. Filters NAME9   -> LOAN.NAME9   / ILOAN.NAME9
                (same ACCTNO keys as NAME8)
             9. Filters LNNAME  -> NAME.LNNAME  / INAME.LNNAME
                (inner-join against ACCTNO keys from routed LNNOTE)

           Routing rule:
             COSTCTR in [3000, 4999] -> Islamic (ILOAN / INAME)
             Otherwise               -> Conventional (LOAN  / NAME)
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
from pathlib import Path

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import duckdb
import polars as pl

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path(".")

# Input working datasets (produced by EIBLNOTE.py, stored in NPL work area)
NPL_DIR    = BASE_DIR / "data" / "pibb" / "npl_woff"   # work datasets from EIBLNOTE.py

# Output library paths
LOAN_DIR   = BASE_DIR / "data" / "pibb" / "loan"        # LOAN  library: SAP.PIBB.*
ILOAN_DIR  = BASE_DIR / "data" / "pibb" / "iloan"       # ILOAN library: SAP.PIBB.ILOAN.*
NAME_DIR   = BASE_DIR / "data" / "pibb" / "name"        # NAME  library: conventional names
INAME_DIR  = BASE_DIR / "data" / "pibb" / "iname"       # INAME library: Islamic names

for _d in [LOAN_DIR, ILOAN_DIR, NAME_DIR, INAME_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# HELPERS
# ============================================================================
def _read(path: Path) -> pl.DataFrame:
    """Read a parquet file; return empty DataFrame if not found."""
    if not path.exists():
        return pl.DataFrame()
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}')"
    ).pl()


def _is_islamic(costctr) -> bool:
    """Return True if COSTCTR is in Islamic range [3000, 4999]."""
    try:
        c = int(costctr or 0)
        return 3000 <= c <= 4999
    except (TypeError, ValueError):
        return False


# ============================================================================
# STEP 1: Split LNNOTE -> NOTE/INOTE (active) + RVRSE/IRVRSE (reversed)
# DATA NOTE INOTE RVRSE IRVRSE;
#   SET LNNOTE;
#   IF REVERSED ^= 'Y' THEN DO;
#      IF (3000<=COSTCTR<=4999) THEN OUTPUT INOTE; ELSE OUTPUT NOTE;
#   END; ELSE DO;
#      IF (3000<=COSTCTR<=4999) THEN OUTPUT IRVRSE; ELSE OUTPUT RVRSE;
#   END;
# PROC SORT DATA=NOTE   OUT=LOAN.LNNOTE;  BY ACCTNO NOTENO;
# PROC SORT DATA=INOTE  OUT=ILOAN.LNNOTE; BY ACCTNO NOTENO;
# PROC SORT DATA=RVRSE  OUT=LOAN.RVNOTE;  BY ACCTNO NOTENO;
# PROC SORT DATA=IRVRSE OUT=ILOAN.RVNOTE; BY ACCTNO NOTENO;
# ============================================================================
def split_lnnote() -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split LNNOTE into conventional (LOAN.*) and Islamic (ILOAN.*) subsets,
    and further into active (LNNOTE) and reversed (RVNOTE).
    Returns (loan_lnnote, iloan_lnnote) for downstream use.
    """
    lnnote = _read(NPL_DIR / "lnnote.parquet")
    if lnnote.is_empty():
        for p in (LOAN_DIR/"lnnote.parquet", ILOAN_DIR/"lnnote.parquet",
                  LOAN_DIR/"rvnote.parquet", ILOAN_DIR/"rvnote.parquet"):
            pl.DataFrame().write_parquet(str(p))
        return pl.DataFrame(), pl.DataFrame()

    note_rows    = []
    inote_rows   = []
    rvrse_rows   = []
    irvrse_rows  = []

    for row in lnnote.iter_rows(named=True):
        reversed_flag = (row.get("REVERSED") or "").strip()
        costctr       = row.get("COSTCTR") or 0
        if reversed_flag != "Y":
            if _is_islamic(costctr):
                inote_rows.append(row)
            else:
                note_rows.append(row)
        else:
            if _is_islamic(costctr):
                irvrse_rows.append(row)
            else:
                rvrse_rows.append(row)

    def _to_df_sorted(rows):
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows).sort(["ACCTNO", "NOTENO"])

    loan_lnnote  = _to_df_sorted(note_rows)
    iloan_lnnote = _to_df_sorted(inote_rows)
    loan_rvnote  = _to_df_sorted(rvrse_rows)
    iloan_rvnote = _to_df_sorted(irvrse_rows)

    loan_lnnote.write_parquet(str(LOAN_DIR  / "lnnote.parquet"))
    iloan_lnnote.write_parquet(str(ILOAN_DIR / "lnnote.parquet"))
    loan_rvnote.write_parquet(str(LOAN_DIR  / "rvnote.parquet"))
    iloan_rvnote.write_parquet(str(ILOAN_DIR / "rvnote.parquet"))

    print(f"LOAN.LNNOTE:   {len(loan_lnnote):,} rows")
    print(f"ILOAN.LNNOTE:  {len(iloan_lnnote):,} rows")
    print(f"LOAN.RVNOTE:   {len(loan_rvnote):,} rows")
    print(f"ILOAN.RVNOTE:  {len(iloan_rvnote):,} rows")

    return loan_lnnote, iloan_lnnote


# ============================================================================
# STEP 2: Split LNACCT -> LOAN.LNACCT / ILOAN.LNACCT
# DATA ACCT IACCT;
#   SET LNACCT;
#   IF (3000<=COSTCTR<=4999) THEN OUTPUT IACCT; ELSE OUTPUT ACCT;
# PROC SORT DATA=ACCT  OUT=LOAN.LNACCT;  BY ACCTNO;
# PROC SORT DATA=IACCT OUT=ILOAN.LNACCT; BY ACCTNO;
# ============================================================================
def split_lnacct() -> None:
    lnacct = _read(NPL_DIR / "lnacct.parquet")
    if lnacct.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "lnacct.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "lnacct.parquet"))
        return

    acct_rows  = []
    iacct_rows = []
    for row in lnacct.iter_rows(named=True):
        if _is_islamic(row.get("COSTCTR") or 0):
            iacct_rows.append(row)
        else:
            acct_rows.append(row)

    loan_lnacct  = pl.DataFrame(acct_rows).sort("ACCTNO")  if acct_rows  else pl.DataFrame()
    iloan_lnacct = pl.DataFrame(iacct_rows).sort("ACCTNO") if iacct_rows else pl.DataFrame()

    loan_lnacct.write_parquet(str(LOAN_DIR  / "lnacct.parquet"))
    iloan_lnacct.write_parquet(str(ILOAN_DIR / "lnacct.parquet"))
    print(f"LOAN.LNACCT:   {len(loan_lnacct):,} rows")
    print(f"ILOAN.LNACCT:  {len(iloan_lnacct):,} rows")


# ============================================================================
# STEP 3: Split LNACC4 -> LOAN.LNACC4 / ILOAN.LNACC4
# DATA ACC4 IACC4;
#   SET LNACC4;
#   IF (3000<=COSTCTR<=4999) THEN OUTPUT IACC4; ELSE OUTPUT ACC4;
# PROC SORT DATA=ACC4  OUT=LOAN.LNACC4;  BY ACCTNO;
# PROC SORT DATA=IACC4 OUT=ILOAN.LNACC4; BY ACCTNO;
# ============================================================================
def split_lnacc4() -> None:
    lnacc4 = _read(NPL_DIR / "lnacc4.parquet")
    if lnacc4.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "lnacc4.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "lnacc4.parquet"))
        return

    acc4_rows  = []
    iacc4_rows = []
    for row in lnacc4.iter_rows(named=True):
        if _is_islamic(row.get("COSTCTR") or 0):
            iacc4_rows.append(row)
        else:
            acc4_rows.append(row)

    loan_lnacc4  = pl.DataFrame(acc4_rows).sort("ACCTNO")  if acc4_rows  else pl.DataFrame()
    iloan_lnacc4 = pl.DataFrame(iacc4_rows).sort("ACCTNO") if iacc4_rows else pl.DataFrame()

    loan_lnacc4.write_parquet(str(LOAN_DIR  / "lnacc4.parquet"))
    iloan_lnacc4.write_parquet(str(ILOAN_DIR / "lnacc4.parquet"))
    print(f"LOAN.LNACC4:   {len(loan_lnacc4):,} rows")
    print(f"ILOAN.LNACC4:  {len(iloan_lnacc4):,} rows")


# ============================================================================
# STEP 4: Filter LIAB -> LOAN.LIAB / ILOAN.LIAB
# Inner-join LIAB against ACCTNO/NOTENO keys from LOAN.LNNOTE / ILOAN.LNNOTE.
#
# DATA NOTE; KEEP ACCTNO NOTENO; SET LOAN.LNNOTE;
# PROC SORT DATA=LIAB; BY ACCTNO NOTENO;
# DATA LOAN.LIAB; MERGE LIAB(IN=A) NOTE(IN=B); BY ACCTNO NOTENO; IF A AND B;
# DATA INOTE; KEEP ACCTNO NOTENO; SET ILOAN.LNNOTE;
# DATA ILOAN.LIAB; MERGE LIAB(IN=A) INOTE(IN=B); BY ACCTNO NOTENO; IF A AND B;
# ============================================================================
def filter_liab(loan_lnnote: pl.DataFrame,
                iloan_lnnote: pl.DataFrame) -> None:
    liab = _read(NPL_DIR / "liab.parquet")
    if liab.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "liab.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "liab.parquet"))
        return

    # NOTE = ACCTNO/NOTENO keys from LOAN.LNNOTE
    def _filter_liab_by_keys(keys_df: pl.DataFrame, out_path: Path) -> None:
        if keys_df.is_empty():
            pl.DataFrame().write_parquet(str(out_path))
            return
        keys = keys_df.select(["ACCTNO","NOTENO"]).unique()
        result = liab.join(keys, on=["ACCTNO","NOTENO"], how="inner")
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    _filter_liab_by_keys(loan_lnnote,  LOAN_DIR  / "liab.parquet")
    _filter_liab_by_keys(iloan_lnnote, ILOAN_DIR / "liab.parquet")


# ============================================================================
# STEP 5: Filter PEND -> LOAN.PEND / ILOAN.PEND
# PROC SORT DATA=PEND; BY ACCTNO NOTENO;
# DATA LOAN.PEND;  MERGE PEND(IN=A) NOTE(IN=B);  BY ACCTNO NOTENO; IF A AND B;
# DATA ILOAN.PEND; MERGE PEND(IN=A) INOTE(IN=B); BY ACCTNO NOTENO; IF A AND B;
# ============================================================================
def filter_pend(loan_lnnote: pl.DataFrame,
                iloan_lnnote: pl.DataFrame) -> None:
    pend = _read(NPL_DIR / "pend.parquet")
    if pend.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "pend.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "pend.parquet"))
        return

    def _filter_pend_by_keys(keys_df: pl.DataFrame, out_path: Path) -> None:
        if keys_df.is_empty():
            pl.DataFrame().write_parquet(str(out_path))
            return
        keys = keys_df.select(["ACCTNO","NOTENO"]).unique()
        result = pend.join(keys, on=["ACCTNO","NOTENO"], how="inner")
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    _filter_pend_by_keys(loan_lnnote,  LOAN_DIR  / "pend.parquet")
    _filter_pend_by_keys(iloan_lnnote, ILOAN_DIR / "pend.parquet")


# ============================================================================
# STEP 6: Filter LNCOMM -> LOAN.LNCOMM / ILOAN.LNCOMM
#
# PROC SORT DATA=LNCOMM; BY ACCTNO COMMNO;
# PROC SORT DATA=ILOAN.LNNOTE OUT=ICOMM (KEEP=ACCTNO COMMNO) NODUPKEY; BY ACCTNO COMMNO;
# PROC SORT DATA=LOAN.LNNOTE  OUT=COMM  (KEEP=ACCTNO COMMNO) NODUPKEY; BY ACCTNO COMMNO;
# DATA LOAN.LNCOMM;
#   MERGE LNCOMM(IN=A) COMM(IN=B); BY ACCTNO COMMNO;
#   IF A AND B THEN OUTPUT;
#   IF A AND NOT B THEN DO;
#     IF (2000000000<=ACCTNO<=2899999999) OR
#        (8000000000<=ACCTNO<=8899999999) THEN OUTPUT;
#   END;
# DATA ILOAN.LNCOMM;
#   MERGE LNCOMM(IN=A) ICOMM(IN=B); BY ACCTNO COMMNO;
#   IF A AND B THEN OUTPUT;
#   IF A AND NOT B THEN DO;
#     IF (2900000000<=ACCTNO<=2999999999) OR
#        (8900000000<=ACCTNO<=8999999999) THEN OUTPUT;
#   END;
# ============================================================================
def filter_lncomm(loan_lnnote: pl.DataFrame,
                  iloan_lnnote: pl.DataFrame) -> None:
    lncomm = _read(NPL_DIR / "lncomm.parquet")
    if lncomm.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "lncomm.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "lncomm.parquet"))
        return

    def _get_comm_keys(note_df: pl.DataFrame) -> pl.DataFrame:
        """Deduplicated ACCTNO/COMMNO keys from a LNNOTE subset."""
        if note_df.is_empty() or "COMMNO" not in note_df.columns:
            return pl.DataFrame()
        return (
            note_df
            .select(["ACCTNO","COMMNO"])
            .unique(subset=["ACCTNO","COMMNO"])
        )

    comm  = _get_comm_keys(loan_lnnote)
    icomm = _get_comm_keys(iloan_lnnote)

    def _filter_lncomm(comm_keys: pl.DataFrame,
                        passthrough_ranges: list[tuple[int, int]],
                        out_path: Path) -> None:
        """
        Output row if matched by COMM key (A AND B),
        OR if unmatched (A AND NOT B) but ACCTNO falls in passthrough ranges.
        """
        if comm_keys.is_empty():
            # No key set: only passthrough ranges apply
            rows_out = []
            for row in lncomm.iter_rows(named=True):
                acctno = row.get("ACCTNO") or 0
                if any(lo <= acctno <= hi for lo, hi in passthrough_ranges):
                    rows_out.append(row)
            result = pl.DataFrame(rows_out) if rows_out else pl.DataFrame()
            result.write_parquet(str(out_path))
            print(f"{out_path.name}: {len(result):,} rows")
            return

        # Inner-join to find matched rows
        matched = lncomm.join(comm_keys, on=["ACCTNO","COMMNO"], how="inner")

        # Anti-join to find unmatched rows, then apply passthrough filter
        unmatched = lncomm.join(comm_keys, on=["ACCTNO","COMMNO"], how="anti")
        passthrough_rows = []
        for row in unmatched.iter_rows(named=True):
            acctno = row.get("ACCTNO") or 0
            if any(lo <= acctno <= hi for lo, hi in passthrough_ranges):
                passthrough_rows.append(row)
        passthrough_df = pl.DataFrame(passthrough_rows) if passthrough_rows else pl.DataFrame()

        result = pl.concat([matched, passthrough_df], how="diagonal") \
            if not passthrough_df.is_empty() else matched
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    # LOAN.LNCOMM passthrough: (2000000000-2899999999) OR (8000000000-8899999999)
    _filter_lncomm(
        comm,
        [(2_000_000_000, 2_899_999_999), (8_000_000_000, 8_899_999_999)],
        LOAN_DIR / "lncomm.parquet"
    )

    # ILOAN.LNCOMM passthrough: (2900000000-2999999999) OR (8900000000-8999999999)
    _filter_lncomm(
        icomm,
        [(2_900_000_000, 2_999_999_999), (8_900_000_000, 8_999_999_999)],
        ILOAN_DIR / "lncomm.parquet"
    )


# ============================================================================
# STEP 7: Filter NAME8 -> LOAN.NAME8 / ILOAN.NAME8
# PROC SORT DATA=NAME8; BY ACCTNO;
# PROC SORT DATA=ILOAN.LNNOTE OUT=INAME(KEEP=ACCTNO) NODUPKEY; BY ACCTNO;
# PROC SORT DATA=LOAN.LNNOTE  OUT=NAME (KEEP=ACCTNO) NODUPKEY; BY ACCTNO;
# DATA LOAN.NAME8;  MERGE NAME8(IN=A) NAME(IN=B);  BY ACCTNO; IF A AND B;
# DATA ILOAN.NAME8; MERGE NAME8(IN=A) INAME(IN=B); BY ACCTNO; IF A AND B;
# ============================================================================
def filter_name8(loan_lnnote: pl.DataFrame,
                 iloan_lnnote: pl.DataFrame) -> None:
    name8 = _read(NPL_DIR / "name8.parquet")
    if name8.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "name8.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "name8.parquet"))
        return

    def _acctno_keys(note_df: pl.DataFrame) -> pl.DataFrame:
        if note_df.is_empty():
            return pl.DataFrame()
        return note_df.select("ACCTNO").unique()

    def _filter_by_acctno(keys: pl.DataFrame, out_path: Path) -> None:
        if keys.is_empty():
            pl.DataFrame().write_parquet(str(out_path))
            return
        result = name8.join(keys, on="ACCTNO", how="inner")
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    _filter_by_acctno(_acctno_keys(loan_lnnote),  LOAN_DIR  / "name8.parquet")
    _filter_by_acctno(_acctno_keys(iloan_lnnote), ILOAN_DIR / "name8.parquet")


# ============================================================================
# STEP 8: Filter NAME9 -> LOAN.NAME9 / ILOAN.NAME9
# DATA LOAN.NAME9;  MERGE NAME9(IN=A) NAME(IN=B);  BY ACCTNO; IF A AND B;
# DATA ILOAN.NAME9; MERGE NAME9(IN=A) INAME(IN=B); BY ACCTNO; IF A AND B;
# (reuses same NAME/INAME ACCTNO keys derived for NAME8)
# ============================================================================
def filter_name9(loan_lnnote: pl.DataFrame,
                 iloan_lnnote: pl.DataFrame) -> None:
    name9 = _read(NPL_DIR / "name9.parquet")
    if name9.is_empty():
        pl.DataFrame().write_parquet(str(LOAN_DIR  / "name9.parquet"))
        pl.DataFrame().write_parquet(str(ILOAN_DIR / "name9.parquet"))
        return

    def _filter_by_acctno(note_df: pl.DataFrame, out_path: Path) -> None:
        if note_df.is_empty():
            pl.DataFrame().write_parquet(str(out_path))
            return
        keys   = note_df.select("ACCTNO").unique()
        result = name9.join(keys, on="ACCTNO", how="inner")
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    _filter_by_acctno(loan_lnnote,  LOAN_DIR  / "name9.parquet")
    _filter_by_acctno(iloan_lnnote, ILOAN_DIR / "name9.parquet")


# ============================================================================
# STEP 9: Filter LNNAME -> NAME.LNNAME / INAME.LNNAME
# PROC SORT DATA=LNNAME; BY ACCTNO;
# DATA NAME.LNNAME;  MERGE LNNAME(IN=A) NAME(IN=B);  BY ACCTNO; IF A AND B;
# DATA INAME.LNNAME; MERGE LNNAME(IN=A) INAME(IN=B); BY ACCTNO; IF A AND B;
# ============================================================================
def filter_lnname(loan_lnnote: pl.DataFrame,
                  iloan_lnnote: pl.DataFrame) -> None:
    lnname = _read(NPL_DIR / "lnname.parquet")
    if lnname.is_empty():
        pl.DataFrame().write_parquet(str(NAME_DIR  / "lnname.parquet"))
        pl.DataFrame().write_parquet(str(INAME_DIR / "lnname.parquet"))
        return

    def _filter_by_acctno(note_df: pl.DataFrame, out_path: Path) -> None:
        if note_df.is_empty():
            pl.DataFrame().write_parquet(str(out_path))
            return
        keys   = note_df.select("ACCTNO").unique()
        result = lnname.join(keys, on="ACCTNO", how="inner")
        result.write_parquet(str(out_path))
        print(f"{out_path.name}: {len(result):,} rows")

    _filter_by_acctno(loan_lnnote,  NAME_DIR  / "lnname.parquet")
    _filter_by_acctno(iloan_lnnote, INAME_DIR / "lnname.parquet")


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("EIBLNEXT started.")

    # Step 1: Split LNNOTE -> LOAN.LNNOTE / ILOAN.LNNOTE / LOAN.RVNOTE / ILOAN.RVNOTE
    print("Splitting LNNOTE...")
    loan_lnnote, iloan_lnnote = split_lnnote()

    # Step 2: Split LNACCT -> LOAN.LNACCT / ILOAN.LNACCT
    print("Splitting LNACCT...")
    split_lnacct()

    # Step 3: Split LNACC4 -> LOAN.LNACC4 / ILOAN.LNACC4
    print("Splitting LNACC4...")
    split_lnacc4()

    # Step 4: Filter LIAB -> LOAN.LIAB / ILOAN.LIAB
    print("Filtering LIAB...")
    filter_liab(loan_lnnote, iloan_lnnote)

    # Step 5: Filter PEND -> LOAN.PEND / ILOAN.PEND
    print("Filtering PEND...")
    filter_pend(loan_lnnote, iloan_lnnote)

    # Step 6: Filter LNCOMM -> LOAN.LNCOMM / ILOAN.LNCOMM (with passthrough ranges)
    print("Filtering LNCOMM...")
    filter_lncomm(loan_lnnote, iloan_lnnote)

    # Step 7: Filter NAME8 -> LOAN.NAME8 / ILOAN.NAME8
    print("Filtering NAME8...")
    filter_name8(loan_lnnote, iloan_lnnote)

    # Step 8: Filter NAME9 -> LOAN.NAME9 / ILOAN.NAME9
    print("Filtering NAME9...")
    filter_name9(loan_lnnote, iloan_lnnote)

    # Step 9: Filter LNNAME -> NAME.LNNAME / INAME.LNNAME
    print("Filtering LNNAME...")
    filter_lnname(loan_lnnote, iloan_lnnote)

    print("EIBLNEXT completed successfully.")


if __name__ == "__main__":
    main()
