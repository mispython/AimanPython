
import pyreadstat
import pandas as pd
from pathlib import Path
import sys

# ============================================================
# CONFIGURATION
# ============================================================

SAS_FILE_PATH = Path(
    "/stgsrcsys/host/uat/AII/MNIFD/enrh_dp_fd_cert_d12.sas7bdat"
)

SAMPLE_ROWS = 5

# Maximum characters displayed for each sample value
MAX_VALUE_LENGTH = 30


# ============================================================
# READ SAS DATASET
# ============================================================

def read_sas_columns(file_path):

    if not file_path.is_file():
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    if file_path.suffix.lower() != ".sas7bdat":
        print("ERROR: File must have a .sas7bdat extension.")
        sys.exit(1)

    try:
        # ----------------------------------------------------
        # STEP 1: READ METADATA ONLY
        # ----------------------------------------------------

        print("Reading SAS dataset metadata...")

        _, metadata = pyreadstat.read_sas7bdat(
            str(file_path),
            metadataonly=True
        )

        column_names = metadata.column_names
        column_types = metadata.readstat_variable_types

        # ----------------------------------------------------
        # STEP 2: READ SAMPLE OBSERVATIONS ONLY
        # ----------------------------------------------------

        print(f"Reading first {SAMPLE_ROWS} observations...")

        sample_df, _ = pyreadstat.read_sas7bdat(
            str(file_path),
            row_limit=SAMPLE_ROWS
        )

        # ----------------------------------------------------
        # STEP 3: DISPLAY COLUMN INFORMATION
        # ----------------------------------------------------

        print("\n" + "=" * 130)
        print("SAS DATASET COLUMN INFORMATION")
        print("=" * 130)

        print(f"File          : {file_path.name}")
        print(f"Full Path     : {file_path}")
        print(f"Total Columns : {len(column_names)}")
        print(f"Sample Rows   : {len(sample_df)}")

        print("=" * 130)

        print(
            f"{'NO.':<6}"
            f"{'COLUMN NAME':<40}"
            f"{'SAS DATATYPE':<20}"
            f"SAMPLE VALUES"
        )

        print("-" * 130)

        # ----------------------------------------------------
        # STEP 4: PRINT EVERY COLUMN
        # ----------------------------------------------------

        for index, column_name in enumerate(column_names, start=1):

            sas_type = column_types.get(column_name, "UNKNOWN")

            sample_values = []

            for value in sample_df[column_name].tolist():

                if pd.isna(value):
                    display_value = "MISSING"
                else:
                    display_value = str(value)

                # Prevent excessively long values from
                # making the output difficult to read
                if len(display_value) > MAX_VALUE_LENGTH:
                    display_value = (
                        display_value[:MAX_VALUE_LENGTH - 3] + "..."
                    )

                sample_values.append(display_value)

            sample_text = " | ".join(sample_values)

            print(
                f"{index:<6}"
                f"{column_name:<40}"
                f"{sas_type:<20}"
                f"{sample_text}"
            )

        # ----------------------------------------------------
        # STEP 5: VALIDATION
        # ----------------------------------------------------

        print("-" * 130)

        print(f"Total columns in metadata : {len(column_names)}")
        print(f"Total columns displayed   : {len(column_names)}")

        if len(column_names) == len(sample_df.columns):
            print("COLUMN VALIDATION         : PASSED")
        else:
            print("COLUMN VALIDATION         : FAILED")

        print("=" * 130)

        print("\nInspection completed.")

    except Exception as error:
        print("\nERROR: Unable to inspect SAS dataset.")
        print(f"Details: {error}")
        sys.exit(1)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    read_sas_columns(SAS_FILE_PATH)
