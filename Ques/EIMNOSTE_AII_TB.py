#!/usr/bin/env python3
"""
Program : EIMNOSTE.py
Purpose : Report on Foreign Exchange Transaction
          Processes Walker and Deposit nostro files and generates summary report
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path

from REPTDATE import get_reptdate_values
from input_date import get_latest_file
from output_date import build_output_file

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

INPUT_DIR  = BASE_DIR / "input" / "prod"
OUTPUT_DIR = BASE_DIR / "output" / "EIMNOSTE"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input paths
INPUT_WKNOST = INPUT_DIR / "FIDSAS.txt"        # Walker file  (fixed-width text)
INPUT_DPNOST = INPUT_DIR / "NOSCBNK.parquet"   # Deposit file (Parquet)

# Output paths
OUTPUT_WAK_DATASET = OUTPUT_DIR / "wak_{year}{month}.parquet"
OUTPUT_DP_DATASET  = OUTPUT_DIR / "dp_{year}{month}.parquet"
OUTPUT_REPORT      = build_output_file(OUTPUT_DIR, "EIMNOSTE_report").with_suffix(".txt")
OUTPUT_SFTP_SCRIPT = OUTPUT_DIR / "sftp_commands.txt"

# Report configuration
PAGE_LENGTH = 60


# ============================================================================
# PACKED DECIMAL UTILITIES
# ============================================================================

def unpack_pd(data: bytes, length: int, decimals: int = 0) -> float:
    """Unpack IBM packed decimal format."""
    if not data or len(data) < length:
        return 0.0

    pd_bytes = data[:length]
    result   = ""

    for byte in pd_bytes:
        high = (byte >> 4) & 0x0F
        low  =  byte       & 0x0F
        if high <= 9:
            result += str(high)
        if low  <= 9:
            result += str(low)

    # Handle sign nibble
    last_nibble = pd_bytes[-1] & 0x0F
    is_negative = last_nibble in (0x0B, 0x0D)

    # Remove the sign nibble digit
    if result:
        result = result[:-1]

    if not result:
        return 0.0

    try:
        value = float(result)
        if decimals > 0:
            value /= 10 ** decimals
        if is_negative:
            value = -value
        return value
    except (ValueError, TypeError):
        return 0.0


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect(database=":memory:")


# ============================================================================
# STEP 1: DERIVE REPORT DATE (REPTDATE module — no .parquet read)
# ============================================================================
print("Step 1: Deriving report date...")

reptdate_values = get_reptdate_values()

reptdate = reptdate_values.reptdate
reptday  = reptdate.day

# Determine week based on day of month (original SAS logic)
if 5 <= reptday <= 10:
    NOWK = "1"
elif 11 <= reptday <= 18:
    NOWK = "2"
elif 19 <= reptday <= 25:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = reptdate_values.reptyear                          # 2-digit year
REPTMON  = reptdate_values.reptmon                           # zero-padded month
REPTDAY  = reptdate_values.reptday                           # zero-padded day
RDATE    = reptdate.strftime("%d/%m/%y")
FILDT    = reptdate.strftime("%d%m%y")

print(f"  Report Date : {RDATE}")
print(f"  Week        : {NOWK}")
print(f"  Year        : {REPTYEAR}  Month : {REPTMON}  Day : {REPTDAY}")
print(f"  File Date   : {FILDT}")


# ============================================================================
# STEP 2: READ WALKER FILE  (.txt — fixed-width)
# ============================================================================
print("\nStep 2: Reading Walker file...")

walker_records: list[dict] = []

try:
    with open(INPUT_WKNOST, "r") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            def _int(s: str) -> int:
                s = s.strip()
                return int(s) if s else 0

            def _str(s: str) -> str:
                return s.strip()

            sbc      = _int(line[0:8])
            dd       = _int(line[8:10])
            mm       = _int(line[11:13])
            yy       = _int(line[14:16])
            dd1      = _int(line[16:18])
            mm1      = _int(line[19:21])
            yy1      = _int(line[22:24])
            nbr      = _str(line[24:48])
            desc     = _str(line[48:108])
            forcur   = _int(line[108:121])
            sign     = _str(line[121:122])
            rmcur    = _int(line[122:133])
            agentno  = _str(line[133:136])
            curcode  = _str(line[136:139])
            trancode = _str(line[139:142])
            name     = _str(line[142:172]) if len(line) >= 172 else ""

            # Implied-decimal scaling
            forcur_f = forcur / 100.0
            rmcur_f  = rmcur  / 100.0

            # Build dates with YEARCUTOFF=1950 (2-digit year → full year)
            try:
                trxdt = (
                    datetime(2000 + yy, mm, dd)
                    if yy < 50
                    else datetime(1900 + yy, mm, dd)
                ) if yy and mm and dd else None
            except (ValueError, TypeError):
                trxdt = None

            try:
                preffdt = (
                    datetime(2000 + yy1, mm1, dd1)
                    if yy1 < 50
                    else datetime(1900 + yy1, mm1, dd1)
                ) if yy1 and mm1 and dd1 else None
            except (ValueError, TypeError):
                preffdt = None

            # Apply debit sign
            if sign == "D":
                forcur_f = -forcur_f
                rmcur_f  = -rmcur_f

            walker_records.append({
                "SBC"     : sbc,
                "NBR"     : nbr,
                "DESC"    : desc,
                "FORCUR"  : forcur_f,
                "SIGN"    : sign,
                "RMCUR"   : rmcur_f,
                "AGENTNO" : agentno,
                "CURCODE" : curcode,
                "TRANCODE": trancode,
                "NAME"    : name,
                "TRXDT"   : trxdt,
                "PREFFDT" : preffdt,
            })

except FileNotFoundError:
    print(f"  Warning: Walker file not found: {INPUT_WKNOST}")

print(f"  Walker records loaded: {len(walker_records):,}")

# Build Polars DataFrame and register with DuckDB
if walker_records:
    df_walker = pl.DataFrame(walker_records)
else:
    df_walker = pl.DataFrame(schema={
        "SBC"     : pl.Int64,
        "NBR"     : pl.Utf8,
        "DESC"    : pl.Utf8,
        "FORCUR"  : pl.Float64,
        "SIGN"    : pl.Utf8,
        "RMCUR"   : pl.Float64,
        "AGENTNO" : pl.Utf8,
        "CURCODE" : pl.Utf8,
        "TRANCODE": pl.Utf8,
        "NAME"    : pl.Utf8,
        "TRXDT"   : pl.Date,
        "PREFFDT" : pl.Date,
    })

con.register("walker_raw", df_walker)

# Add TRDESC via DuckDB CASE expression
walker_df = con.execute("""
    SELECT
        SBC, NBR, "DESC", FORCUR, SIGN, RMCUR,
        AGENTNO, CURCODE, TRANCODE, NAME, TRXDT, PREFFDT,
        CASE TRANCODE
            WHEN '001' THEN 'OUTWARD DD'
            WHEN '003' THEN 'OUTWARD MT'
            WHEN '006' THEN 'OUTWARD TT'
            WHEN '011' THEN 'BANK GUARANTEE'
            WHEN '013' THEN 'ECRF PRE SHIPMENT'
            WHEN '015' THEN 'ECRF POST SHIPMENT'
            WHEN '021' THEN 'FBEP CLEAN'
            WHEN '023' THEN 'FBEP DOCUMENTARY'
            WHEN '025' THEN 'FBEP AP'
            WHEN '031' THEN 'FOBC CLEAN'
            WHEN '033' THEN 'OBC DOCUMENTARY'
            WHEN '041' THEN 'IBC CLEAN'
            WHEN '043' THEN 'IBC DOCUMENTARY'
            WHEN '051' THEN 'BR'
            WHEN '055' THEN 'DPC'
            WHEN '061' THEN 'LC'
            WHEN '063' THEN 'ILC'
            WHEN '081' THEN 'INWARD DD'
            WHEN '083' THEN 'INWARD MT'
            WHEN '085' THEN 'INWARD TT'
            WHEN '091' THEN 'MISCELLANEOUS'
            WHEN '092' THEN 'TREASURY CREDIT'
            WHEN '093' THEN 'TREASURY DEBIT'
            ELSE 'OTHERS'
        END AS TRDESC
    FROM walker_raw
""").pl()

con.register("walker_with_desc", walker_df)

# Save Walker dataset
output_wak = str(OUTPUT_WAK_DATASET).format(year=REPTYEAR, month=REPTMON)
Path(output_wak).parent.mkdir(parents=True, exist_ok=True)
con.execute(f"COPY walker_with_desc TO '{output_wak}' (FORMAT PARQUET)")
print(f"  Walker dataset saved : {output_wak}")


# ============================================================================
# STEP 3: READ DEPOSIT FILE  (.parquet)
# ============================================================================
print("\nStep 3: Reading Deposit file...")

print("\n",
    con.execute(f"""
        DESCRIBE
        SELECT * FROM read_parquet('{INPUT_DPNOST}')
        LIMIT 5
    """).fetchdf(), "\n"
)

deposit_df = con.execute(f"""
    WITH filtered AS (
        SELECT *
        FROM read_parquet('{INPUT_DPNOST}')
        WHERE NOT (ACCTNO >= 3997200109 AND ACCTNO <= 3997204029)
    )
    SELECT
        ACCTNO,
        COALESCE(NULLIF(TRIM(AGENTNO), ''), '000') AS AGENTNO,
        NAME,
        TRIND,
        TRTYPE,
        SIGN,
        -- Apply sign to amounts
        CASE 
            WHEN SIGN = 'D' 
            THEN -COALESCE(TRY_CAST(FORCUR AS DOUBLE), 0)
            ELSE  COALESCE(TRY_CAST(FORCUR AS DOUBLE), 0)
        END AS FORCUR,

        CASE 
            WHEN SIGN = 'D' 
            THEN -COALESCE(TRY_CAST(RMCUR AS DOUBLE), 0)
            ELSE  COALESCE(TRY_CAST(RMCUR AS DOUBLE), 0) 
        END AS RMCUR,
        CURCODE,
        BILLIND,
        CASE
            WHEN YY IS NOT NULL
            AND MM IS NOT NULL
            AND DD IS NOT NULL
            THEN MAKE_DATE(
                CASE WHEN YY < 50 THEN 2000 + YY ELSE 1900 + YY END,
                MM,
                DD
            )
        END AS STARTDT,

        CASE
            WHEN YY1 IS NOT NULL
            AND MM1 IS NOT NULL
            AND DD1 IS NOT NULL
            THEN MAKE_DATE(
                CASE WHEN YY1 < 50 THEN 2000 + YY1 ELSE 1900 + YY1 END,
                MM1,
                DD1
            )
        END AS ENDDT,

        CASE
            WHEN YY2 IS NOT NULL
            AND MM2 IS NOT NULL
            AND DD2 IS NOT NULL
            THEN MAKE_DATE(
                CASE WHEN YY2 < 50 THEN 2000 + YY2 ELSE 1900 + YY2 END,
                MM2,
                DD2
            )
        END AS TRXDT,
        CASE
            WHEN BILLIND IN ('L','X','I','O','G') THEN
                CASE BILLIND
                    WHEN 'L' THEN 'IMPORT LC'
                    WHEN 'X' THEN 'EXPORT LC'
                    WHEN 'I' THEN 'INWARD BILLS COLL'
                    WHEN 'O' THEN 'OUTWARD BILL COLL'
                    WHEN 'G' THEN 'BANK GUARANTEE'
                END
            WHEN TRIND = 'O' THEN
                CASE
                    WHEN TRTYPE = 'TT'            THEN 'OUTWARD TT'
                    WHEN TRTYPE = 'DD'            THEN 'OUTWARD DD'
                    WHEN TRTYPE IN ('MT', 'TF')   THEN 'OUTWARD MT'
                    ELSE 'OTHERS'
                END
            WHEN TRIND = 'R' THEN
                CASE
                    WHEN TRTYPE = 'TT' THEN 'REPLACE TT'
                    WHEN TRTYPE = 'DD' THEN 'REPLACE DD'
                    WHEN TRTYPE = 'MT' THEN 'REPLACE MT'
                    ELSE 'OTHERS'
                END
            WHEN TRIND = 'I' THEN
                CASE
                    WHEN TRTYPE = 'TT' THEN 'INWARD TT'
                    WHEN TRTYPE = 'DD' THEN 'INWARD DD'
                    ELSE 'OTHERS'
                END
            WHEN TRIND = 'B' THEN
                CASE
                    WHEN TRTYPE = 'TT' THEN 'B BACK TT'
                    WHEN TRTYPE = 'DD' THEN 'B BACK DD'
                    ELSE 'OTHERS'
                END
            WHEN TRIND = 'C' THEN
                CASE
                    WHEN TRTYPE = 'TT' THEN 'CANCEL TT'
                    WHEN TRTYPE = 'DD' THEN 'CANCEL DD'
                    ELSE 'OTHERS'
                END
            ELSE 'OTHERS'
        END AS TRDESC
    FROM filtered
""").pl()

con.register("deposit_with_desc", deposit_df)

print(f"  Deposit records loaded : {len(deposit_df):,}")

# Save Deposit dataset
output_dp = str(OUTPUT_DP_DATASET).format(year=REPTYEAR, month=REPTMON)
Path(output_dp).parent.mkdir(parents=True, exist_ok=True)
con.execute(f"COPY deposit_with_desc TO '{output_dp}' (FORMAT PARQUET)")
print(f"  Deposit dataset saved : {output_dp}")


# ============================================================================
# STEP 4: MERGE NAMES FROM WALKER AND DEPOSIT
# ============================================================================
print("\nStep 4: Merging names...")

allname_df = con.execute("""
    WITH combined AS (
        SELECT AGENTNO, NAME FROM walker_with_desc
        UNION ALL
        SELECT AGENTNO, NAME FROM deposit_with_desc
    ),
    ranked AS (
        SELECT AGENTNO, NAME,
               ROW_NUMBER() OVER (PARTITION BY AGENTNO ORDER BY NAME) AS rn
        FROM combined
    )
    SELECT AGENTNO, NAME
    FROM ranked
    WHERE rn = 1
""").pl()

con.register("allname", allname_df)
print(f"  Unique agent names : {len(allname_df):,}")


# ============================================================================
# STEP 5: COMBINE WALKER AND DEPOSIT DATA
# ============================================================================
print("\nStep 5: Combining Walker and Deposit data...")

allrec_df = con.execute("""
    WITH combined AS (
        SELECT AGENTNO, CURCODE, TRDESC, SIGN, FORCUR, RMCUR, TRXDT
        FROM walker_with_desc
        UNION ALL
        SELECT AGENTNO, CURCODE, TRDESC, SIGN, FORCUR, RMCUR, TRXDT
        FROM deposit_with_desc
    )
    SELECT c.*, n.NAME
    FROM combined c
    LEFT JOIN allname n ON c.AGENTNO = n.AGENTNO
""").pl()

con.register("allrec", allrec_df)
print(f"  Combined records : {len(allrec_df):,}")


# ============================================================================
# STEP 6: SUMMARIZE DATA
# ============================================================================
print("\nStep 6: Summarizing data...")

summary_df = con.execute("""
    SELECT
        CURCODE,
        AGENTNO,
        NAME,
        TRDESC,
        SIGN,
        COUNT(*)       AS NOTRAN,
        SUM(FORCUR)    AS FORCUR,
        SUM(RMCUR)     AS RMCUR
    FROM allrec
    GROUP BY CURCODE, AGENTNO, NAME, TRDESC, SIGN
    ORDER BY CURCODE, AGENTNO, NAME, TRDESC, SIGN
""").pl()

print(f"  Summary records : {len(summary_df):,}")


# ============================================================================
# STEP 7: GENERATE REPORT  (ASA carriage-control characters)
# ============================================================================
# ASA carriage-control conventions used:
#   '1'  — advance to top of next page before printing
#   ' '  — advance one line before printing  (single spacing)
#   '0'  — advance two lines before printing (double spacing)
#   '+'  — no advance (overprint)
# ============================================================================
print("\nStep 7: Generating report...")

Path(OUTPUT_REPORT).parent.mkdir(parents=True, exist_ok=True)

line_count  = 0
page_number = 0


# ============================================================================
# REPORT COLUMN WIDTHS - Define proper column widths
# ============================================================================

CURCODE_W = 8
AGENTNO_W = 5
NAME_W    = 30
TRDESC_W  = 20
SIGN_W    = 9

# Requested:
# Leave 4 spaces between SIGN and NOTRAN
GAP1_W    = 4

NOTRAN_W  = 12
FORCUR_W  = 17
RMCUR_W   = 17

# Fix subtotal dashed line allignment
NUMERIC_SECTION_W = (
    NOTRAN_W +
    FORCUR_W +
    RMCUR_W +
    2
)

DASH_W = FORCUR_W + RMCUR_W + 1


# Fix header column allignment
def _write_page_header(fh, rdate: str) -> int:
    """Write the report page header."""

    fh.write(f"REPORT ID : EIMNOSTE\n")
    fh.write(f"REPORT ON FOREIGN EXCHANGE TRANSACTION AS AT {rdate}\n")
    fh.write(" \n")

    fh.write(
        f" {'CURRENCY':<{CURCODE_W}} "
        f"{'AGENT':<{AGENTNO_W}} "
        f"{'NAME':<{NAME_W}} "
        f"{'TRANSACTION':<{TRDESC_W}} "
        f"{'DEBIT(D)/':<{SIGN_W}}"
        f"{'':<{GAP1_W}}"
        f"{'NO OF':>{NOTRAN_W}} "
        f"{'FOREIGN':>{FORCUR_W}} "
        f"{'RM':>{RMCUR_W}}\n"
    )

    fh.write(
        f" {'CODE':<{CURCODE_W}} "
        f"{'NO':<{AGENTNO_W}} "
        f"{'':<{NAME_W}} "
        f"{'DESCRIPTION':<{TRDESC_W}} "
        f"{'CREDIT(C)':<{SIGN_W}}"
        f"{'':<{GAP1_W}}"
        f"{'TRANS':>{NOTRAN_W}} "
        f"{'AMOUNT':>{FORCUR_W}} "
        f"{'AMOUNT':>{RMCUR_W}}\n"
    )

    fh.write(
        f" {'':<{CURCODE_W}} "
        f"{'':<{AGENTNO_W}} "
        f"{'':<{NAME_W}} "
        f"{'':<{TRDESC_W}} "
        f"{'':<{SIGN_W}}"
        f"{'':<{GAP1_W}}"
        f"{'ACTION':>{NOTRAN_W}} "
        f"{'':>{FORCUR_W}} "
        f"{'':>{RMCUR_W}}\n"
    )

    fh.write(f" {'-' * 132}\n")

    return 7


with open(OUTPUT_REPORT, "w") as fh:
    line_count  = _write_page_header(fh, RDATE)
    page_number = 1

    current_curcode  = None
    current_agentno  = None
    agent_forcur     = 0.0
    agent_rmcur      = 0.0
    total_forcur     = 0.0
    total_rmcur      = 0.0

    rows = summary_df.to_dicts()

    for i, row in enumerate(rows):
        # ── page break check ──────────────────────────────────────────────
        if line_count >= PAGE_LENGTH:
            line_count  = _write_page_header(fh, RDATE)
            page_number += 1

        # ── agent break ───────────────────────────────────────────────────
        if current_agentno is not None and row["AGENTNO"] != current_agentno:
            # Fix agent subtotal allignment
            subtotal_indent = (
                1 +
                CURCODE_W + 1 +
                AGENTNO_W + 1 +
                NAME_W + 1 +
                TRDESC_W + 1 +
                SIGN_W +
                GAP1_W +
                NOTRAN_W + 1
            )
          
            fh.write(f"{' ' * subtotal_indent}{'-' * DASH_W}\n")
            fh.write(
                f"{' ' * subtotal_indent}"
                f"{agent_forcur:>{FORCUR_W},.2f} "
                f"{agent_rmcur:>{RMCUR_W},.2f}\n"
            )
            fh.write(f"{' ' * subtotal_indent}{'-' * DASH_W}\n")
            fh.write(" \n")
            line_count   += 4
            agent_forcur  = 0.0
            agent_rmcur   = 0.0

        # ── detail line ───────────────────────────────────────────────────
        detail = " "                                                         # ASA single-space

        if row["CURCODE"] != current_curcode:
            detail          += f"{row['CURCODE']:<8} "
            current_curcode  =  row["CURCODE"]
        else:
            detail += " " * 9

        if row["AGENTNO"] != current_agentno:
            detail          += f"{row['AGENTNO']:<5} "
            current_agentno  =  row["AGENTNO"]
        else:
            detail += " " * 6

        name   = (row["NAME"]   or "")[:30]
        trdesc = (row["TRDESC"] or "")[:20]
        sign   =  row["SIGN"]   or ""
        notran = int(row["NOTRAN"] or 0)
        forcur = float(row["FORCUR"] or 0.0)
        rmcur  = float(row["RMCUR"]  or 0.0)

          # Fix detail row allignment
        detail += (
              f"{name:<{NAME_W}} "
              f"{trdesc:<{TRDESC_W}} "
              f"{sign:<{SIGN_W}}"
              f"{'':<{GAP1_W}}"
              f"{notran:>{NOTRAN_W},} "
              f"{forcur:>{FORCUR_W},.2f} "
              f"{rmcur:>{RMCUR_W},.2f}\n"
          )
        fh.write(detail)
        line_count += 1

        # Accumulate
        agent_forcur += forcur
        agent_rmcur  += rmcur
        total_forcur += forcur
        total_rmcur  += rmcur

    # ── final agent subtotal ──────────────────────────────────────────────
    if current_agentno is not None:
          # Fix final agent subtotal allignment
          subtotal_indent = (
          1 +
          CURCODE_W + 1 +
          AGENTNO_W + 1 +
          NAME_W + 1 +
          TRDESC_W + 1 +
          SIGN_W +
          GAP1_W +
          NOTRAN_W + 1
          )
          
          fh.write(f"{' ' * subtotal_indent}{'-' * DASH_W}\n")
          fh.write(
          f"{' ' * subtotal_indent}"
          f"{agent_forcur:>{FORCUR_W},.2f} "
          f"{agent_rmcur:>{RMCUR_W},.2f}\n"
          )
          fh.write(f"{' ' * subtotal_indent}{'-' * DASH_W}\n")
        fh.write(" \n")

    # ── grand total ───────────────────────────────────────────────────────
    # Fix grand total allignment
          grand_indent = (
              1 +
              CURCODE_W + 1 +
              AGENTNO_W + 1 +
              NAME_W + 1 +
              TRDESC_W + 1
          )
          
          fh.write(
              f"{' ' * grand_indent}"
              f"{'TOTAL':>{SIGN_W + GAP1_W + NOTRAN_W}} "
              f"{total_forcur:>{FORCUR_W},.2f} "
              f"{total_rmcur:>{RMCUR_W},.2f}\n"
          )
          
          fh.write(
              f"{' ' * subtotal_indent}"
              f"{'=' * DASH_W}\n"
          )
    fh.write(f" {' ' * 94}{'=' * 36}\n")

print(f"  Report saved : {OUTPUT_REPORT}")


# ============================================================================
# STEP 8: GENERATE SFTP SCRIPT
# ============================================================================
print("\nStep 8: Generating SFTP script...")

Path(OUTPUT_SFTP_SCRIPT).parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_SFTP_SCRIPT, "w") as fh:
    fh.write(f"PUT //SAP.PBB.EIMNOSTE.TEXT(+1) EIMNOSTE_{FILDT}.TXT\n")

print(f"  SFTP script saved : {OUTPUT_SFTP_SCRIPT}")


# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("FOREIGN EXCHANGE TRANSACTION REPORT COMPLETE")
print("=" * 70)
print(f"\n  Report Date         : {RDATE}")
print(f"  Week                : {NOWK}")
print(f"  Total Transactions  : {len(allrec_df):,}")
print(f"  Summary Records     : {len(summary_df):,}")

print("\nGenerated Files:")
print(f"  1. Walker Dataset   : {output_wak}")
print(f"  2. Deposit Dataset  : {output_dp}")
print(f"  3. Report           : {OUTPUT_REPORT}")
print(f"  4. SFTP Script      : {OUTPUT_SFTP_SCRIPT}")

# Transaction totals
if len(summary_df) > 0:
    totals = con.execute("""
        SELECT
            SUM(FORCUR)           AS total_forcur,
            SUM(RMCUR)            AS total_rmcur,
            COUNT(DISTINCT CURCODE)  AS num_currencies,
            COUNT(DISTINCT AGENTNO)  AS num_agents
        FROM summary_df
    """).fetchone()

    print("\nTransaction Summary:")
    print(f"  Total Foreign Amount : {totals[0]:,.2f}" if totals[0] else "  Total Foreign Amount : 0.00")
    print(f"  Total RM Amount      : {totals[1]:,.2f}" if totals[1] else "  Total RM Amount      : 0.00")
    print(f"  Number of Currencies : {totals[2]}")
    print(f"  Number of Agents     : {totals[3]}")

print("\nConversion complete!")

# Close DuckDB connection
con.close()
