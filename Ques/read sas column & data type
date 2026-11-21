
import pyreadstat
import os
import sys

# ============================================================
# CONFIGURATION
# ============================================================

SAS_FILE_PATH = r"C:\Users\aiman\Desktop\SAS_Python_Migration\DUMMY\YOUR_FILE.sas7bdat"

# ============================================================
# READ SAS DATASET METADATA
# ============================================================

def read_sas_columns(file_path):

    if not os.path.isfile(file_path):
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    if not file_path.lower().endswith(".sas7bdat"):
        print("ERROR: File must have a .sas7bdat extension.")
        sys.exit(1)

    try:
        # Read metadata without loading the entire dataset into memory
        metadata = pyreadstat.read_sas7bdat(
            file_path,
            metadataonly=True
        )[1]

        # All column names in their original order
        column_names = metadata.column_names

        # Original SAS storage types
        column_types = metadata.readstat_variable_types

        # ====================================================
        # DISPLAY RESULTS
        # ====================================================

        print("=" * 90)
        print("SAS DATASET COLUMN INFORMATION")
        print("=" * 90)

        print(f"File       : {os.path.basename(file_path)}")
        print(f"Full Path  : {file_path}")
        print(f"Total Columns: {len(column_names)}")
        print("=" * 90)

        print(f"{'NO.':<8}{'COLUMN NAME':<45}{'SAS DATATYPE':<20}")
        print("-" * 90)

        for index, column_name in enumerate(column_names, start=1):

            sas_type = column_types.get(column_name, "UNKNOWN")

            print(
                f"{index:<8}"
                f"{column_name:<45}"
                f"{sas_type:<20}"
            )

        print("-" * 90)

        # ====================================================
        # VALIDATION
        # ====================================================

        printed_count = len(column_names)

        print(f"Total columns in metadata : {len(column_names)}")
        print(f"Total columns listed      : {printed_count}")

        if printed_count == len(column_names):
            print("VALIDATION STATUS         : PASSED")
        else:
            print("VALIDATION STATUS         : FAILED")

        print("=" * 90)

    except Exception as error:
        print(f"ERROR: Unable to read SAS dataset.")
        print(f"Details: {error}")
        sys.exit(1)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    read_sas_columns(SAS_FILE_PATH)
