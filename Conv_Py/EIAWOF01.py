# !/usr/bin/env python3
"""
Program: EIAWOF01
"""

import shutil
from pathlib import Path

# Setup paths
INPUT_FILE = "SAP.PBB.NPL.HP.SASDATA.parquet"
OUTPUT_WOFF = "SAP.PBB.NPL.HP.SASDATA.WOFF.parquet"
OUTPUT_BAK = "SAP.PBB.NPL.HP.SASDATA.BAK.parquet"


def main():
    """
    This program copies the input file to two output files.
    Equivalent to JCL IEBGENER copy operation.
    """
    input_path = Path(INPUT_FILE)
    output_woff_path = Path(OUTPUT_WOFF)
    output_bak_path = Path(OUTPUT_BAK)

    # Delete existing output files if they exist (equivalent to DELETE step)
    if output_woff_path.exists():
        output_woff_path.unlink()
        print(f"Deleted existing file: {OUTPUT_WOFF}")

    if output_bak_path.exists():
        output_bak_path.unlink()
        print(f"Deleted existing file: {OUTPUT_BAK}")

    # Verify input file exists
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    # Copy input file to WOFF output
    shutil.copy2(input_path, output_woff_path)
    print(f"Copied {INPUT_FILE} to {OUTPUT_WOFF}")

    # Copy input file to BAK output
    shutil.copy2(input_path, output_bak_path)
    print(f"Copied {INPUT_FILE} to {OUTPUT_BAK}")

    print("\nCopy operations completed successfully")


if __name__ == "__main__":
    main()
