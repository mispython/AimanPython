#!/usr/bin/env python3
"""
Program : EIBMNPL0.py
Purpose : JCL job driver for the "TOTAL OVERDUE LOANS" report suite.
          Original JCL: //EIBMNPL0 JOB MISEIS,EIBMNPL0,COND=(0,LT),...

          Steps in the original JCL:
            1. DELETE  (PGM=IEFBR14) - DISP=(MOD,DELETE,DELETE) on
               SAP.PBB.ODTLLIST.COLD, SAP.PBB.ODTLLIST.TEXT,
               SAP.PIBB.ODTLLIST.COLD, SAP.PIBB.ODTLLIST.TEXT
            2. CREATE  (PGM=IEFBR14) - DISP=(NEW,CATLG,DELETE) allocates
               empty SAP.PBB.ODTLLIST.TEXT / SAP.PIBB.ODTLLIST.TEXT
               (RECFM=FB,LRECL=256,BLKSIZE=25600)
            3. EIBMNPL0 EXEC SAS609 (PBB step)  - //BNM DD DSN=SAP.PBB.SASDATA,
               //BNM1 DD DSN=SAP.PBB.MNILN(0), //OD DD DSN=SAP.PBB.MNILIMT(0),
               //ODTLLIST DD DSN=SAP.PBB.ODTLLIST.COLD (RECFM=FBA,LRECL=136),
               //TEMP DD DSN=SAP.PBB.ODTLLIST.TEXT, SYSIN = member EIBMNPL1
            4. EIBMNPL0 EXEC SAS609 (PIBB step) - identical to step 3 but
               every DD points at the SAP.PIBB.* library set instead

          JOBPARM S=S1M2 / PRINT1 OUTPUT CLASS=R / SASLIST DD (commented out
          in the JCL) are spool/print-routing directives (destination
          printer, copies, room/building/department address) with no
          filesystem equivalent in this migration and are intentionally not
          reproduced here.

Dependency:
    EIBMNPL1.py already merges the PBB step and the PIBB step of this job
    into ONE Python process: its module-level ENTITIES = ("PBB", "PIBB")
    tuple and the loop inside EIBMNPL1.main() run both entities in turn,
    and EIBMNPL1.main() already drives EIBMNPL2 per entity (via
    "import EIBMNPL2" + EIBMNPL2.run(entity, asa)) in lieu of the original
    %INC PGM1(EIBMNPL2). Steps 3 and 4 of this job are therefore BOTH
    satisfied by a single call to EIBMNPL1.main() below -- there is no
    separate "PBB step" / "PIBB step" left for this driver to invoke.

    EIBMNPL1.main() is guarded by "if __name__ == '__main__':" inside
    EIBMNPL1.py, so a bare "import EIBMNPL1" would only execute its
    module-level caching code (Steps 1-2 of EIBMNPL1) and NOT the actual
    report-generation loop. To truthfully trigger the child program (the
    JCL-driver equivalent of "SYSIN DD DSN=SAP.BNM.PROGRAM(EIBMNPL1)"),
    this module imports EIBMNPL1 and explicitly calls EIBMNPL1.main().

    %INC PGM(PBBLNFMT,PBBELF) and %INC PGM1(EIBMNPL2) are EIBMNPL1's own
    dependencies (already handled inside EIBMNPL1.py / EIBMNPL2.py) and are
    not re-declared here.
"""

import sys
from pathlib import Path

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
OUTPUT_DIR = BASE_DIR / "output" / "EIBMNPL"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ENTITIES = ("PBB", "PIBB")

# Catalogued-dataset equivalents (per entity):
#   SAP.<ENTITY>.ODTLLIST.COLD -> <ENTITY>_ODTLLIST_COLD.txt (ASA, RECFM=FBA)
#   SAP.<ENTITY>.ODTLLIST.TEXT -> <ENTITY>_ODTLLIST_TEXT.txt (plain, RECFM=FB)
COLD_FILES = [OUTPUT_DIR / f"{e}_ODTLLIST_COLD.txt" for e in ENTITIES]
TEXT_FILES = [OUTPUT_DIR / f"{e}_ODTLLIST_TEXT.txt" for e in ENTITIES]


# ============================================================================
# STEP 1: DELETE  (PGM=IEFBR14 -- DISP=(MOD,DELETE,DELETE) on all 4 datasets)
# ============================================================================
def _delete_step() -> None:
    print("Step 1 [DELETE]: Removing old ODTLLIST.COLD / ODTLLIST.TEXT outputs...")
    for path in COLD_FILES + TEXT_FILES:
        if path.exists():
            path.unlink()
            print(f"  Deleted   : {path.name}")
        else:
            print(f"  Not found : {path.name} (skip)")


# ============================================================================
# STEP 2: CREATE  (PGM=IEFBR14 -- DISP=(NEW,CATLG,DELETE) allocates the two
# ODTLLIST.TEXT datasets empty, ready for EIBMNPL1's PROC PRINTTO PRINT=TEMP
# NEW to open/overwrite). ODTLLIST.COLD is NOT pre-allocated by this JCL
# step (only opened later, inline, by the SAS step's own ODTLLIST DD), so
# only the two .TEXT files are pre-touched here, matching the CRT01/CRT02
# DD statements in the original JCL exactly.
# ============================================================================
def _create_step() -> None:
    print("\nStep 2 [CREATE]: Pre-allocating empty ODTLLIST.TEXT outputs...")
    for path in TEXT_FILES:
        path.touch()
        print(f"  Allocated (empty): {path.name}")


# ============================================================================
# STEP 3/4: EIBMNPL0 EXEC SAS609  (PBB step, then PIBB step)
# Both //BNM DD variants (SAP.PBB.SASDATA / SAP.PIBB.SASDATA) with
# SYSIN=EIBMNPL1 are satisfied by ONE call into EIBMNPL1.main(), which
# already loops ENTITIES=("PBB","PIBB") internally and already drives
# EIBMNPL2 for each entity in turn (see EIBMNPL1.py module docstring/main).
# COND=(4,LT) on the JOB card is emulated, per project convention, by
# halting the whole driver on any unhandled exception from the SAS step
# (no partial-entity continuation on failure).
# ============================================================================
def _run_sas_step() -> None:
    print("\nStep 3/4 [EIBMNPL0 EXEC SAS609]: Running EIBMNPL1 (PBB + PIBB)...")
    import EIBMNPL1
    try:
        EIBMNPL1.main()
    except Exception as exc:
        print(f"\nEIBMNPL0 job FAILED (COND=(4,LT) equivalent): {exc}")
        sys.exit(4)


# ============================================================================
# MAIN JOB DRIVER
# ============================================================================
def main() -> None:
    print("=" * 70)
    print("EIBMNPL0 JOB START")
    print("=" * 70)

    _delete_step()
    _create_step()
    _run_sas_step()

    print("\n" + "=" * 70)
    print("EIBMNPL0 JOB END - RC=0")
    print("=" * 70)


if __name__ == "__main__":
    main()
