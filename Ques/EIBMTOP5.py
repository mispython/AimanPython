#!/usr/bin/env python3
"""
Program : EIBMTOP5.py
Purpose : PB Subsidiaries Under Top 50 Corporate Depositors report
          (originally %INC PGM(EIBMTOP5) inside EIBMLIQP, output
          SAP.PBB.TOP50.TEXT / DD FD2TEXT, RECFM=FB LRECL=320 -- no ASA
          carriage-control byte, per project convention for plain RECFM=FB
          reports; page breaks marked with form-feed).

          %INC PGM(PBBLNFMT,PBBELF,PBBDPFMT) is included by the original
          SAS but no PUT(var,fmt.) call appears anywhere in this program's
          body -- kept as documentation only, per project convention
          (session-level %INC with no direct call -> comment, not import).

          Designed to be imported by EIBMLIQP.py, mirroring %INC
          semantics. Owns no physical path of its own -- every cache path
          and RDATE are supplied by the calling job.
"""
from pathlib import Path

import duckdb
import polars as pl

TOP50_CUSTNOS = (53227, 169990, 170108, 3562038, 3721354)


def run_eibmtop5(
    cisln_deposit_cache: Path,
    deposit_current_cache: Path,
    cisdp_deposit_cache: Path,
    deposit_fd_cache: Path,
    rdate: str,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "TOP50.txt"

    con = duckdb.connect(database=":memory:")
    cisca = con.execute(f"""
        SELECT * FROM read_parquet('{cisln_deposit_cache.as_posix()}')
        WHERE ACCTNO BETWEEN 3000000000 AND 3999999999
    """).pl()
    ca = con.execute(f"SELECT * FROM read_parquet('{deposit_current_cache.as_posix()}')").pl()
    cisfd = con.execute(f"""
        SELECT * FROM read_parquet('{cisdp_deposit_cache.as_posix()}')
        WHERE (ACCTNO BETWEEN 1000000000 AND 1999999999)
           OR (ACCTNO BETWEEN 7000000000 AND 7999999999)
    """).pl()
    fd = con.execute(f"SELECT * FROM read_parquet('{deposit_fd_cache.as_posix()}')").pl()
    con.close()

    ca_prod_excl = {400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410}
    caorg = (
        ca.with_columns(pl.col("CURBAL").alias("CABAL"))
        .join(cisca.sort("ACCTNO"), on="ACCTNO", how="inner")
        .filter((pl.col("PURPOSE") != "2") & (~pl.col("PRODUCT").is_in(ca_prod_excl)) & (pl.col("INDORG") == "O"))
    )

    fd_prod_excl = {350, 351, 352, 353, 354, 355, 356, 357}
    fdorg = (
        fd.with_columns(pl.col("CURBAL").alias("FDBAL"))
        .join(cisfd.sort("ACCTNO"), on="ACCTNO", how="inner")
        .filter((pl.col("PURPOSE") != "2") & (~pl.col("PRODUCT").is_in(fd_prod_excl)) & (pl.col("INDORG") == "O"))
    )

    common_cols = [c for c in fdorg.columns if c in caorg.columns]
    data1 = (
        pl.concat([fdorg.select(common_cols), caorg.select(common_cols)])
        .filter(pl.col("CUSTNO").is_in(TOP50_CUSTNOS))
        .sort(["CUSTNO", "ACCTNO"])
    )

    lines = [
        "PUBLIC BANK BERHAD      PROGRAM-ID: EIBMTOP5",
        f"PB SUBSIDIARIES UNDER TOP 50 CORP DEPOSITORS @ {rdate}",
        "",
    ]
    header = f"{'BRANCH':<8}{'MNI NO':<14}{'DEPOSITOR':<26}{'CIS NO':<10}{'CUSTCD':<8}{'CURRENT BALANCE':>18}  {'PRODUCT':<8}"
    total, current_custno = 0.0, None
    for r in data1.iter_rows(named=True):
        if r["CUSTNO"] != current_custno:
            if current_custno is not None:
                lines.append(f"{'':<66}{total:>18,.2f}")
                lines.append("")
            current_custno, total = r["CUSTNO"], 0.0
            lines.append(header)
        curbal = r.get("CURBAL") or 0.0
        total += curbal
        lines.append(
            f"{str(r.get('BRANCH', '')):<8}{str(r.get('ACCTNO', '')):<14}{str(r.get('CUSTNAME', '')):<26}"
            f"{str(r.get('CUSTNO', '')):<10}{str(r.get('CUSTCODE', '')):<8}{curbal:>18,.2f}  {str(r.get('PRODUCT', '')):<8}"
        )
    if current_custno is not None:
        lines.append(f"{'':<66}{total:>18,.2f}")

    with open(output_file, "w", encoding="latin1") as fh:
        fh.write("\n".join(lines) + "\n")

    print(f"EIBMTOP5 complete. Rows: {len(data1):,}")
    print(f"Output written: {output_file}")
    return output_file
