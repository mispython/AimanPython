# !/usr/bin/env python3
"""
Program: EIAWOF02
"""

import polars as pl
from pathlib import Path

# Setup paths
INPUT_WOFF = "RBP2.B033.LN.WRI2.OFF.MIS.parquet"
OUTPUT_NPL = "SAP.PBB.NPL.HP.SASDATA.WOFF.parquet"
OUTPUT_REPORT = "WOFFHP_REPORT.txt"


def write_report_with_asa(df: pl.DataFrame, output_file: str, page_length: int = 60):
    """
    Write a report with ASA carriage control characters.
    Page length defaults to 60 lines per page.
    """
    with open(output_file, 'w') as f:
        lines_on_page = 0

        # Write header with page eject (ASA '1')
        f.write('1')
        f.write(' ' * 5 + 'OBS' + ' ' * 10 + 'ACCTNO\n')
        f.write(' ')
        f.write('-' * 70 + '\n')
        lines_on_page = 3

        # Write data rows
        for idx, row in enumerate(df.iter_rows(), start=1):
            # Check if we need a new page
            if lines_on_page >= page_length - 2:
                f.write('1')
                f.write(' ' * 5 + 'OBS' + ' ' * 10 + 'ACCTNO\n')
                f.write(' ')
                f.write('-' * 70 + '\n')
                lines_on_page = 3

            # Normal line (ASA ' ')
            f.write(' ')
            acctno = row[0] if row[0] is not None else ''
            f.write(f'{idx:>8}' + ' ' * 10 + f'{acctno}\n')
            lines_on_page += 1


def main():
    # Read the WOFF file
    df_woff = pl.read_parquet(INPUT_WOFF)

    # Extract columns based on column positions from INPUT statement
    # @142 ACCTNO 10. means 10 characters starting at position 142
    # @236 COSTCTR 4. means 4 characters starting at position 236
    woffhp = df_woff.select([
        pl.col("ACCTNO"),
        pl.col("COSTCTR")
    ])

    # Filter: IF ((3000<=COSTCTR<=3999) OR COSTCTR IN (4043,4048)) THEN DELETE;
    # The ELSE OUTPUT means we keep records that don't match the condition
    woffhp = woffhp.filter(
        ~(
                ((pl.col("COSTCTR") >= 3000) & (pl.col("COSTCTR") <= 3999)) |
                (pl.col("COSTCTR") == 4043) |
                (pl.col("COSTCTR") == 4048)
        )
    )

    # Keep only ACCTNO column
    woffhp = woffhp.select("ACCTNO")

    # Sort by ACCTNO
    woffhp = woffhp.sort("ACCTNO")

    # Write output dataset
    woffhp.write_parquet(OUTPUT_NPL)

    # Generate PROC PRINT report with ASA carriage control
    write_report_with_asa(woffhp, OUTPUT_REPORT)

    print(f"Dataset written to: {OUTPUT_NPL}")
    print(f"Report written to: {OUTPUT_REPORT}")
    print(f"Total records: {len(woffhp)}")


if __name__ == "__main__":
    main()
