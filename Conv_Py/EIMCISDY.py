# !/usr/bin/env python3
"""
Program : EIMCISDY.py
Purpose : Convert files into SAS Datawarehouse for survey responses
          for prize winners — Dataset: CUSTDLY

ESMR    : 2018-4001

Original JCL Steps:
  DELETE    (IEFBR14) — Delete SAP.PBB.MONTH.CUSTDLY.CUSFTP
  CREATE    (IEFBR14) — Allocate SAP.PBB.MONTH.CUSTDLY.CUSFTP
                        (FB, LRECL=80, BLKSIZE=27920)
  EIMCISDY  (SAS609)  — SAS program (this conversion)
  RUNSFTP   (COZBATCH)— FTP outputs to EDW; external step, not converted.

Inputs:
  CUST.CUSTDLY — SAS dataset (RBP2.B033.CIS.CUST.DAILY); converted to Parquet
  CISTAXID     — Fixed-width mainframe flat file (RBP2.B033.CCRIS.TAXID.GDG(0));
                 maintained as .txt per project convention.
                 Layout (1-based, SAS INPUT notation):
                   @001 CUSTNO    $11.  → bytes [0:11]   (0-based)
                   @083 RHOLD_IND $1.  → bytes [82:83]  (0-based)

Output:
  CUSTDLY.parquet — Merged result with columns:
                    ACCTNOC, CUSTNO, RHOLD_IND, CUSTNAME, DOBDOR, ALIAS

Note: PROC CPORT (SAP.PBB.MONTH.CUSTDLY.CUSFTP) writes a SAS binary
        transport file — a mainframe-only SAS utility that cannot be
        reproduced in Python. The merged CUSTDLY dataset is written as
        Parquet instead, which supersedes the CPORT file in the
        Parquet-based migration pipeline.
"""

from pathlib import Path
import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
BASE_OUT          = Path("output_parquet")
BASE_OUT.mkdir(parents=True, exist_ok=True)

# CUST.CUSTDLY — SAS dataset source; converted to Parquet
CUST_CUSTDLY_PATH = Path("/host/cis/parquet/CIS_CUST_DAILY/year=2025/month=9/day=17/data_0.parquet")

# CISTAXID — fixed-width mainframe flat file; maintained as .txt
# Layout: CUSTNO at @001 ($11.), RHOLD_IND at @083 ($1.)
CISTAXID_PATH     = Path("/host/ccris/flat/CCRIS_TAXID_GDG_0.txt")

# Output Parquet (replaces PROC CPORT / SAP.PBB.MONTH.CUSTDLY.CUSFTP)
OUT_PATH          = BASE_OUT / "MONTH" / "CUSTDLY.parquet"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Fixed-width layout constants for CISTAXID flat file (1-based → 0-based)
# @001 CUSTNO    $11.  → [0:11]
# @083 RHOLD_IND $1.  → [82:83]
# MISSOVER: short records silently produce missing values for absent columns
# ---------------------------------------------------------------------------
_CUSTNO_START    = 0    # inclusive
_CUSTNO_END      = 11   # exclusive  (11 chars)
_RHOLD_IND_START = 82   # inclusive
_RHOLD_IND_END   = 83   # exclusive  (1 char)


def _read_cistaxid(path: Path) -> pl.DataFrame:
    """
    Equivalent of:
      DATA CISTAXID;
        INFILE CISTAXID MISSOVER;
        INPUT @001 CUSTNO $11.  @083 RHOLD_IND $1.;
      RUN;
      PROC SORT DATA=CISTAXID NODUPKEY; BY CUSTNO; RUN;

    Reads the fixed-width flat file, extracts CUSTNO and RHOLD_IND,
    then deduplicates by CUSTNO keeping the first occurrence (NODUPKEY).
    MISSOVER: records shorter than byte 83 yield a blank RHOLD_IND.
    """
    rows: list[dict] = []

    with open(path, 'rb') as f:
        for raw_line in f:
            # Strip newline bytes but preserve internal spaces (fixed-width)
            line = raw_line.rstrip(b'\r\n')

            custno    = line[_CUSTNO_START:_CUSTNO_END].decode('ascii', errors='replace').strip()
            # MISSOVER: if record too short for RHOLD_IND, treat as missing ('')
            if len(line) > _RHOLD_IND_START:
                rhold_ind = line[_RHOLD_IND_START:_RHOLD_IND_END].decode('ascii', errors='replace')
            else:
                rhold_ind = ''

            if custno:   # skip completely blank lines
                rows.append({'CUSTNO': custno, 'RHOLD_IND': rhold_ind})

    df = pl.DataFrame(rows, schema={'CUSTNO': pl.Utf8, 'RHOLD_IND': pl.Utf8})

    # PROC SORT NODUPKEY BY CUSTNO — keep first occurrence per CUSTNO
    df = (
        df
        .sort('CUSTNO')
        .unique(subset=['CUSTNO'], keep='first')
    )

    return df


def main() -> None:
    # -------------------------------------------------------------------------
    # 1) Read CUST.CUSTDLY via DuckDB (SAS dataset → Parquet)
    #    Equivalent of: PROC SORT DATA=CUST.CUSTDLY OUT=CUSTDLY; BY CUSTNO;
    #    Note: Pre-sort on CUSTNO is not required for a Polars join;
    #          removed per project convention (eliminate unnecessary sorts).
    # -------------------------------------------------------------------------
    con = duckdb.connect()
    CUSTDLY = con.execute(
        f"SELECT * FROM read_parquet('{CUST_CUSTDLY_PATH}')"
    ).pl()
    con.close()

    # -------------------------------------------------------------------------
    # 2) Read CISTAXID fixed-width flat file and deduplicate
    #    Equivalent of:
    #      DATA CISTAXID; INFILE CISTAXID MISSOVER;
    #        INPUT @001 CUSTNO $11.  @083 RHOLD_IND $1.; RUN;
    #      PROC SORT DATA=CISTAXID NODUPKEY; BY CUSTNO; RUN;
    # -------------------------------------------------------------------------
    CISTAXID = _read_cistaxid(CISTAXID_PATH)

    # -------------------------------------------------------------------------
    # 3) MERGE CUSTDLY(IN=A) CISTAXID(IN=B); BY CUSTNO; IF A;
    #    Left join: keep all CUSTDLY rows; bring in RHOLD_IND where matched.
    #    KEEP: ACCTNOC CUSTNO RHOLD_IND CUSTNAME DOBDOR ALIAS
    # -------------------------------------------------------------------------
    CUSTDLY_MERGED = (
        CUSTDLY
        .join(
            CISTAXID.select(['CUSTNO', 'RHOLD_IND']),
            on='CUSTNO',
            how='left',
        )
        .select(['ACCTNOC', 'CUSTNO', 'RHOLD_IND', 'CUSTNAME', 'DOBDOR', 'ALIAS'])
    )

    # -------------------------------------------------------------------------
    # 4) Write output as Parquet
    #    Replaces: FILENAME TRANFILE '...CUSFTP'; PROC CPORT DATA=CUSTDLY
    #              FILE=TRANFILE; RUN;
    #    PROC CPORT produces a SAS binary transport file — mainframe-only and
    #    not reproducible in Python. Parquet is the pipeline equivalent.
    # -------------------------------------------------------------------------
    CUSTDLY_MERGED.write_parquet(OUT_PATH)
    print(f"[EIMCISDY] Output written: {OUT_PATH}  ({len(CUSTDLY_MERGED)} rows)")


if __name__ == "__main__":
    main()
