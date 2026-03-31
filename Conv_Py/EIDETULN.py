# !/usr/bin/env python3
"""
Program : EIDETULN.py
Purpose : Creating control file for Detica AML Solution
          Bank Control - Loan Segment

Original JCL Steps:
  1. DELETE   (IEFBR14)  - Delete existing unload dataset
                           DSN=SAP.PBB.BCLNSAPR.UNLOAD
  2. ULBCFXC  (DFSRRC00) - IMS database unload utility
                           Unloads IMS physical database BCLNSAPR
                           (IMSRBP2.IB330P.BCLNSAPR) to a variable-blocked
                           (VB) flat file (SAP.PBB.BCLNSAPR.UNLOAD.VB)
                           using IMS utility DFSURGU0.
                           Secondary index: IMSRBP2.IB330P.BCLNSAI1
  3. FIXBLCK  (SORT)     - Convert VB records to fixed-block (FB, LRECL=1000)
                           OUTFIL CONVERT,OUTREC=(5,1000):
                             - Skip the 4-byte RDW (Record Descriptor Word)
                               prepended to each VB record by the IMS unloader
                             - Start at byte 5, take 1000 bytes → fixed-width
                               FB record
  4. DELETE   (IEFBR14)  - Clean up temporary VB unload file

Migration Notes:
  This program contains NO SAS logic. It is a pure mainframe JCL job that:
    - Invokes the IMS database unload utility (DFSRRC00/DFSURGU0) to extract
        raw IMS segment data from the proprietary BCLNSAPR IMS database.
    - Reformats the variable-blocked (VB) output to fixed-block (FB) by
        stripping the 4-byte Record Descriptor Word (RDW) from each record.

  In the Python migration context, the upstream data extraction from the IMS
    database (BCLNSAPR - Loan segment) is assumed to have been performed
    externally and the resulting data made available as a Parquet file.
  This program's unload/reformat steps are therefore superseded by the
    Parquet-based pipeline; no functional Python conversion is required.

  Difference from EIDETUDP (Deposit segment):
    - Source IMS database : BCLNSAPR  (Loan)   vs BCDEPAPR  (Deposit)
    - Secondary index DSN : BCLNSAI1  (Loan)   vs BCDEPAIX  (Deposit)
    - Dataset names       : BCLNSAPR.*         vs BCDEPAPR.*
  The RDW-stripping logic (FIXBLCK) is identical between both programs.

  If a raw VB flat file must be post-processed (e.g. for legacy interface
    compatibility), the RDW-stripping logic below demonstrates the equivalent
    of the FIXBLCK SORT step.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
BASE_DIR        = Path(os.environ.get("BASE_DIR", "/data/detica"))
INPUT_VB_FILE   = BASE_DIR / "BCLNSAPR.UNLOAD.VB.bin"   # Raw IMS VB unload
OUTPUT_FB_FILE  = BASE_DIR / "BCLNSAPR.UNLOAD.dat"      # Fixed-block output

LRECL      = 1000   # Fixed logical record length (LRECL=1000 per JCL DCB)
RDW_LENGTH = 4      # IMS variable-blocked Record Descriptor Word is 4 bytes


def delete_output_if_exists(path: Path) -> None:
    """
    Equivalent of JCL DELETE step (IEFBR14 with DISP=(MOD,DELETE,DELETE)).
    Deletes the file if it already exists to ensure a clean run.
    """
    if path.exists():
        path.unlink()
        print(f"[DELETE] Removed existing file: {path}")


def convert_vb_to_fb(input_path: Path, output_path: Path, lrecl: int, rdw_len: int) -> None:
    """
    Equivalent of JCL FIXBLCK step (SORT FIELDS=COPY, OUTFIL CONVERT,OUTREC=(5,1000)).

    Reads a variable-blocked (VB) flat file produced by the IMS unloader
    (DFSURGU0) and writes a fixed-block (FB) file by:
      - Skipping the RDW (first `rdw_len` bytes of each VB record)
      - Taking exactly `lrecl` bytes starting at byte offset `rdw_len`
      - Padding with spaces if the segment data is shorter than LRECL
      - Truncating if the segment data exceeds LRECL

    IMS VB record layout:
      Bytes 1-2 : Record length (big-endian unsigned short, includes RDW itself)
      Bytes 3-4 : Segment flags / reserved
      Bytes 5-N : Actual segment data  ← OUTREC=(5,1000) selects this range

    Note: This step is only needed when consuming the raw IMS unload file
    directly. In the Parquet-based pipeline this conversion is not required.
    """
    if not input_path.exists():
        raise FileNotFoundError(
            f"[ULBCFXC] IMS VB unload file not found: {input_path}\n"
            "This file must be produced externally by the IMS unload utility "
            "(DFSRRC00/DFSURGU0) before this step can run."
        )

    delete_output_if_exists(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records_written = 0

    with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
        while True:
            # Read the 4-byte RDW
            rdw = fin.read(rdw_len)
            if not rdw:
                break  # End of file
            if len(rdw) < rdw_len:
                raise ValueError("Truncated RDW encountered — input file may be corrupt.")

            # RDW bytes 1-2: total record length (big-endian), includes the 4-byte RDW
            total_len = int.from_bytes(rdw[0:2], byteorder="big")
            data_len  = total_len - rdw_len

            if data_len < 0:
                raise ValueError(f"Invalid RDW length field: {total_len}")

            # Read the segment data portion
            segment_data = fin.read(data_len)
            if len(segment_data) < data_len:
                raise ValueError("Truncated segment data — input file may be corrupt.")

            # OUTREC=(5,1000): take exactly LRECL bytes from offset 0 of segment data
            # (offset 5 in 1-based JCL = offset 0 after RDW is stripped)
            fb_record = segment_data[:lrecl].ljust(lrecl)  # pad / truncate to LRECL

            fout.write(fb_record)
            records_written += 1

    print(f"[FIXBLCK] Converted {records_written} VB records → FB (LRECL={lrecl}): {output_path}")


def main() -> None:
    # -----------------------------------------------------------------
    # Step 1: DELETE - Remove existing fixed-block unload file
    # (JCL: //DELETE EXEC PGM=IEFBR14, DSN=SAP.PBB.BCLNSAPR.UNLOAD)
    # -----------------------------------------------------------------
    delete_output_if_exists(OUTPUT_FB_FILE)

    # -----------------------------------------------------------------
    # Step 2: ULBCFXC - IMS database unload (DFSRRC00/DFSURGU0)
    # (JCL: //ULBCFXC EXEC PGM=DFSRRC00,
    #        PARM=(ULU,DFSURGU0,BCLNSAPR,,,,,,,,,,,N,N))
    #
    # This step unloads the IMS physical database BCLNSAPR (Loan segment)
    # to a variable-blocked flat file.  Secondary index BCLNSAI1 is also
    # referenced.  It must be executed externally via the mainframe IMS
    # utilities; it cannot be replicated in Python.
    #
    # In the Parquet-based migration pipeline, the BCLNSAPR database content
    # is assumed to have been extracted and made available as a Parquet file
    # upstream.  This placeholder documents the original IMS unload intent.
    # -----------------------------------------------------------------
    print(
        "[ULBCFXC] IMS unload step: database BCLNSAPR (Loan) → VB flat file.\n"
        "          Secondary index: BCLNSAI1.\n"
        "          This step is performed externally by mainframe IMS utilities.\n"
        "          In the Parquet pipeline, source data is sourced from Parquet."
    )

    # -----------------------------------------------------------------
    # Step 3: FIXBLCK - Convert VB to FB (SORT FIELDS=COPY,
    #         OUTFIL CONVERT,OUTREC=(5,1000))
    #
    # Only execute if the raw VB unload file is present (i.e. when operating
    # in a legacy flat-file mode rather than the Parquet pipeline).
    # -----------------------------------------------------------------
    if INPUT_VB_FILE.exists():
        convert_vb_to_fb(INPUT_VB_FILE, OUTPUT_FB_FILE, LRECL, RDW_LENGTH)

        # -------------------------------------------------------------
        # Step 4: DELETE - Remove temporary VB unload file
        # (JCL: //DELETE EXEC PGM=IEFBR14, DSN=SAP.PBB.BCLNSAPR.UNLOAD.VB)
        # -------------------------------------------------------------
        delete_output_if_exists(INPUT_VB_FILE)
    else:
        print(
            f"[FIXBLCK] Skipped: VB input file not found ({INPUT_VB_FILE}).\n"
            "          Operating in Parquet pipeline mode — no flat-file "
            "conversion required."
        )


if __name__ == "__main__":
    main()
