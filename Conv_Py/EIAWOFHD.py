# !/usr/bin/env python3
"""
Program: EIAWOFHD
"""

import duckdb
import polars as pl
from pathlib import Path

# Setup paths
INPUT_WOFF = "RBP2.B033.LN.WRI2.OFF.MIS.parquet"
INPUT_NPL = "SAP.PBB.NPL.WOFF.parquet"
OUTPUT_NPL = "SAP.PBB.NPL.WOFF.parquet"
REPORT_OUTPUT = "PROC_PRINT_REPORT.txt"


def format_asa_report(df, sum_column=None):
    """Format report with ASA carriage control characters"""
    lines = []
    page_length = 60
    line_count = 0

    # Page header
    lines.append(" " + "Obs".rjust(10) + "ACCTNO".rjust(15) + "WOFFDT".rjust(15) + "CAPBAL".rjust(20))
    line_count += 1

    # Data rows
    for idx, row in enumerate(df.iter_rows(), 1):
        if line_count >= page_length - 2:
            lines.append("1")
            lines.append(" " + "Obs".rjust(10) + "ACCTNO".rjust(15) + "WOFFDT".rjust(15) + "CAPBAL".rjust(20))
            line_count = 1

        acctno = str(row[0]) if row[0] is not None else ""
        woffdt = str(row[1]) if row[1] is not None else ""
        capbal = f"{row[2]:.2f}" if row[2] is not None else ""

        line = " " + str(idx).rjust(10) + acctno.rjust(15) + woffdt.rjust(15) + capbal.rjust(20)
        lines.append(line)
        line_count += 1

    # Summary line
    if sum_column is not None:
        total = df[sum_column].sum()
        lines.append(" " + " " * 10 + "=" * 15 + " " * 15 + "=" * 20)
        lines.append(" " + " " * 40 + f"{total:.2f}".rjust(20))

    return "\n".join(lines)


def main():
    # Read WOFF file using DuckDB
    con = duckdb.connect()

    # Query to extract columns from WOFF parquet file
    hp = con.execute(f"""
        SELECT 
            ACCTNO,
            WOFFDTE,
            CAPBAL,
            COSTCTR
        FROM read_parquet('{INPUT_WOFF}')
    """).pl()

    # Convert WOFFDTE and create WOFFDT column
    hp = hp.with_columns([
        pl.when(pl.col("WOFFDTE").is_not_null())
        .then(
            pl.col("WOFFDTE").str.to_date("%d/%m/%y", strict=False)
            .fill_null(pl.col("WOFFDTE").str.to_date("%d/%m/%Y", strict=False))
        )
        .otherwise(None)
        .alias("woffdte_parsed")
    ])

    hp = hp.with_columns([
        pl.when(pl.col("woffdte_parsed").is_not_null())
        .then(
            pl.col("woffdte_parsed").dt.strftime("%b").str.to_uppercase() +
            pl.lit(" ") +
            pl.col("woffdte_parsed").dt.year().cast(pl.Utf8)
        )
        .otherwise(None)
        .alias("WOFFDT")
    ])

    # Filter: IF COSTCTR NOT IN (3000:3999,4043,4048)
    hp = hp.filter(
        ~(
                ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999)) |
                (pl.col("COSTCTR") == 4043) |
                (pl.col("COSTCTR") == 4048)
        )
    )

    # Drop columns
    hp = hp.select(["ACCTNO", "WOFFDT", "CAPBAL"])

    # Sort by ACCTNO
    hp = hp.sort("ACCTNO")

    # Generate PROC PRINT report
    report = format_asa_report(hp, sum_column="CAPBAL")
    with open(REPORT_OUTPUT, "w") as f:
        f.write(report)

    # Read existing NPL.HPWOFF file
    npl_hpwoff = con.execute(f"""
        SELECT * FROM read_parquet('{INPUT_NPL}')
    """).pl()

    # Combine HP and NPL.HPWOFF
    combined = pl.concat([hp, npl_hpwoff], how="vertical")

    # Remove duplicates by ACCTNO, keeping first occurrence
    final = combined.unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")

    # Write output
    final.write_parquet(OUTPUT_NPL)

    con.close()


if __name__ == "__main__":
    main()
