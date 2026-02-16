# !/usr/bin/env python3
"""
Program: EIFMIISA - GL Walker Interface File Generation
Purpose: Process & Create Interface File for GL Walker
         Generates GL interface file from NPL and Loan data for HPD CONV and HPD AITAB accounts
ESMR: 2004-720 & 2004-579
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION - Define all paths early
# ============================================================================

# Input paths
# INPUT_PATH_REPTDATE = "SAP_PBB_MNILN_0_REPTDATE.parquet"
# INPUT_PATH_NPL_IIS = "SAP_PBB_NPL_HP_SASDATA_IIS.parquet"
# INPUT_PATH_LNNOTE = "SAP_PBB_MNILN_0_LNNOTE.parquet"

BASE_DIR = Path(__file__).resolve().parent

INPUT_PATH_REPTDATE = BASE_DIR / "SAP_PBB_MNILN_0_REPTDATE.parquet"
INPUT_PATH_NPL_IIS = BASE_DIR / "SAP_PBB_NPL_HP_SASDATA_IIS.parquet"
INPUT_PATH_LNNOTE = BASE_DIR / "SAP_PBB_MNILN_0_LNNOTE.parquet"

# Output path
OUTPUT_PATH_GLFILE = "SAP_PFB_NPL_GLFILE.txt"


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    # Initialize DuckDB connection
    con = duckdb.connect()

    # ========================================================================
    # Step 1: Process REPTDATE to generate macro variables
    # ========================================================================

    reptdate_df = pl.read_parquet(INPUT_PATH_REPTDATE)
    reptdate_row = reptdate_df.row(0, named=True)

    reptdate = reptdate_row['REPTDATE']

    # Calculate date/time variables
    dt = datetime.now()

    # Month calculations
    mm1 = reptdate.month - 1
    if mm1 == 0:
        mm1 = 12

    # Format date and time
    datenow = f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"
    timenow = f"{dt.hour:02d}{dt.minute:02d}{dt.second:02d}"

    # Create macro variables dictionary
    macro_vars = {
        'REPTMON': f"{reptdate.month:02d}",
        'PREVMON': f"{mm1:02d}",
        'REPTDAT': reptdate,
        'RPTDTE': datenow,
        'RPTTIME': timenow,
        'CURDAY': f"{dt.day:02d}",
        'CURMTH': f"{dt.month:02d}",
        'CURYR': f"{dt.year:04d}",
        'RPTYR': f"{reptdate.year:04d}",
        'RPTDY': f"{reptdate.day:02d}"
    }

    # ========================================================================
    # Step 2: Load and process NPL.IIS and LNNOTE data
    # ========================================================================

    # Register parquet files with DuckDB
    con.execute(f"CREATE VIEW npliis_raw AS SELECT * FROM read_parquet('{INPUT_PATH_NPL_IIS}')")
    con.execute(f"CREATE VIEW lnnote_raw AS SELECT * FROM read_parquet('{INPUT_PATH_LNNOTE}')")

    # Remove duplicates from NPLIIS (equivalent to PROC SORT NODUPKEYS)
    con.execute("""
        CREATE VIEW npliis_dedup AS 
        SELECT DISTINCT * FROM npliis_raw
        ORDER BY ACCTNO, NOTENO
    """)

    # Merge NPLIIS with LNNOTE
    npliis_df = con.execute("""
        SELECT 
            a.*,
            CASE 
                WHEN b.COSTCTR = 1146 THEN 146
                ELSE b.COSTCTR
            END AS COSTCTR
        FROM npliis_dedup a
        LEFT JOIN lnnote_raw b
        ON a.ACCTNO = b.ACCTNO AND a.NOTENO = b.NOTENO
        ORDER BY COSTCTR
    """).pl()

    # ========================================================================
    # Step 3: Create GLPROC records
    # ========================================================================

    glproc_records = []

    for row in npliis_df.iter_rows(named=True):
        costctr = row['COSTCTR']
        loantyp = row.get('LOANTYP', '')

        # Determine loan type
        is_hpd_conv = loantyp[:8] == 'HPD CONV' if loantyp else False

        # Process each field based on loan type
        if is_hpd_conv:
            # HPD CONV logic
            if row.get('SUSPEND') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISHPDTE',
                    'JDESC': 'HPD CONV INTEREST SUSPENDED DURING THE PERIOD (E)',
                    'AVALUE': row['SUSPEND']
                })

            if row.get('RECOVER') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISWHPDF',
                    'JDESC': 'HPD CONV WRITTEN BACK TO PROFIT & LOSS (F)',
                    'AVALUE': row['RECOVER']
                })

            if row.get('RECC') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISHPDRG',
                    'JDESC': 'HPD CONV REVERSAL OF CURRENT YEAR IIS (G)',
                    'AVALUE': row['RECC']
                })

            if row.get('OISUSP') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISHPDTK',
                    'JDESC': 'HPD CONV OI SUSPENDED DURING THE PERIOD (K)',
                    'AVALUE': row['OISUSP']
                })

            if row.get('OIRECV') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISWHPDL',
                    'JDESC': 'HPD CONV WRITTEN BACK TO PROFIT & LOSS (L)',
                    'AVALUE': row['OIRECV']
                })

            if row.get('OIRECC') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISHPDRM',
                    'JDESC': 'HPD CONV REVERSAL OF CURRENT YEAR IIS (M)',
                    'AVALUE': row['OIRECC']
                })
        else:
            # HPD AITAB logic
            if row.get('SUSPEND') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITAE',
                    'JDESC': 'HPD AITAB INTEREST SUSPENDED DURING THE PERIOD (E)',
                    'AVALUE': row['SUSPEND']
                })

            if row.get('RECOVER') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITWF',
                    'JDESC': 'HPD AITAB WRITTEN BACK TO PROFIT & LOSS (F)',
                    'AVALUE': row['RECOVER']
                })

            if row.get('RECC') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITRG',
                    'JDESC': 'HPD AITAB REVERSAL OF CURRENT YEAR IIS (G)',
                    'AVALUE': row['RECC']
                })

            if row.get('OISUSP') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITAK',
                    'JDESC': 'HPD AITAB OI SUSPENDED DURING THE PERIOD (K)',
                    'AVALUE': row['OISUSP']
                })

            if row.get('OIRECV') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITWL',
                    'JDESC': 'HPD AITAB WRITTEN BACK TO PROFIT & LOSS (L)',
                    'AVALUE': row['OIRECV']
                })

            if row.get('OIRECC') not in (0, None):
                glproc_records.append({
                    'COSTCTR': costctr,
                    'ARID': 'ISAITRM',
                    'JDESC': 'HPD AITAB REVERSAL OF CURRENT YEAR IIS (M)',
                    'AVALUE': row['OIRECC']
                })

    # Create GLPROC dataframe
    if glproc_records:
        glproc_df = pl.DataFrame(glproc_records)

        # Aggregate by COSTCTR, ARID, JDESC
        glproc_df = glproc_df.group_by(['COSTCTR', 'ARID', 'JDESC']).agg(
            pl.col('AVALUE').sum()
        ).sort(['COSTCTR', 'ARID'])
    else:
        glproc_df = pl.DataFrame({
            'COSTCTR': [],
            'ARID': [],
            'JDESC': [],
            'AVALUE': []
        })

    # ========================================================================
    # Step 4: Generate GLFILE output
    # ========================================================================

    with open(OUTPUT_PATH_GLFILE, 'w') as f:
        trn = len(glproc_df)
        cnt = 0
        amttotc = 0.0
        amttotd = 0.0

        # Write header record
        write_header_record(f, macro_vars)

        # Process each record
        if trn > 0:
            prev_arid = None
            brhamt = 0.0

            for idx, row in enumerate(glproc_df.iter_rows(named=True)):
                costctr = row['COSTCTR']
                arid = row['ARID']
                jdesc = row['JDESC']
                avalue = round(row['AVALUE'], 2)

                # Determine sign
                if avalue < 0:
                    avalue = abs(avalue)
                    asign = '-'
                else:
                    asign = '+'

                # Check if first record for this ARID
                if arid != prev_arid:
                    brhamt = 0.0

                brhamt += avalue

                if asign == '-':
                    amttotc += avalue
                else:
                    amttotd += avalue

                # Check if last record for this ARID
                is_last_arid = (idx == len(glproc_df) - 1 or
                                glproc_df.row(idx + 1, named=True)['ARID'] != arid)

                if is_last_arid:
                    cnt += 1
                    write_detail_record(f, arid, costctr, asign, brhamt, jdesc, macro_vars)

                prev_arid = arid

        # Write trailer record
        write_trailer_record(f, macro_vars, amttotd, amttotc, cnt)

    con.close()
    print(f"GL interface file generated successfully: {OUTPUT_PATH_GLFILE}")


def write_header_record(f, macro_vars):
    """Write header record (type '0')"""
    line = (
        f"INTFIISF"
        f"0"
        f"        "
        f"{macro_vars['RPTYR']}"
        f"{macro_vars['REPTMON']}"
        f"{macro_vars['RPTDY']}"
        f"{macro_vars['RPTDTE']}"
        f"{macro_vars['RPTTIME']}"
    )
    f.write(line + '\n')


def write_detail_record(f, arid, costctr, asign, brhamt, jdesc, macro_vars):
    """Write detail record (type '1')"""
    # Format amount (remove decimal point)
    tempamt1 = f"{brhamt:017.2f}"
    brhamto = tempamt1.replace('.', '')

    # Determine date based on ARID
    is_cor = arid[5:8] == 'COR' if len(arid) >= 8 else False

    if is_cor:
        trans_yr = macro_vars['CURYR']
        trans_mth = macro_vars['CURMTH']
        trans_day = macro_vars['CURDAY']
    else:
        trans_yr = macro_vars['RPTYR']
        trans_mth = macro_vars['REPTMON']
        trans_day = macro_vars['RPTDY']

    # Build the record (324 bytes total)
    line = (
        f"INTFIISF"  # 1-8
        f"1"  # 9
        f"{arid:<8}"  # 10-17
        f"PBBH"  # 18-21
        f"{costctr:04d}"  # 22-25
        f"{'':<45}"  # 26-70
        f"MYR"  # 71-73
        f"{'':<3}"  # 74-76
        f"{trans_yr}"  # 77-80
        f"{trans_mth}"  # 81-82
        f"{trans_day}"  # 83-84
        f"{asign}"  # 85
        f"{brhamto}"  # 86-101
        f"{jdesc:<60}"  # 102-161
        f"{arid:<8}"  # 162-169
        f"{'':<2}"  # 170-171
        f"IISL"  # 172-175
        f"{'':<76}"  # 176-251
        f"+"  # 252
        f"0000000000000000"  # 253-268
        f"{'':<4}"  # 269-272
        f"+"  # 273
        f"000000000000000"  # 274-288
        f"{'':<8}"  # 289-296
        f"{trans_yr}"  # 297-300
        f"{trans_mth}"  # 301-302
        f"{trans_day}"  # 303-304
        f"{'':<2}"  # 305-306
        f"+"  # 307
        f"0000000000000000"  # 308-323
        f""  # 324
    )
    f.write(line + '\n')


def write_trailer_record(f, macro_vars, amttotd, amttotc, cnt):
    """Write trailer record (type '9')"""
    # Format amounts (remove decimal point)
    tempamt1 = f"{amttotd:017.2f}"
    amttotdo = tempamt1.replace('.', '')

    tempamt2 = f"{amttotc:017.2f}"
    amttotco = tempamt2.replace('.', '')

    line = (
        f"INTFIISF"  # 1-8
        f"9"  # 9
        f"        "  # 10-17
        f"{macro_vars['RPTYR']}"  # 18-21
        f"{macro_vars['REPTMON']}"  # 22-23
        f"{macro_vars['RPTDY']}"  # 24-25
        f"{macro_vars['RPTDTE']}"  # 26-33
        f"{macro_vars['RPTTIME']}"  # 34-39
        f"+"  # 40
        f"{amttotdo}"  # 41-56
        f"-"  # 57
        f"{amttotco}"  # 58-73
        f"+"  # 74
        f"0000000000000000"  # 75-90
        f"-"  # 91
        f"0000000000000000"  # 92-107
        f"{cnt:09d}"  # 108-116
    )
    f.write(line + '\n')


if __name__ == "__main__":
    main()
