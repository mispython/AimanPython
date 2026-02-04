"""
File Name: EIIMTCOF
Islamic Bank LCR Concentration of Funding Report
Generates LCR Table 4 - Concentration of Funding report for Public Islamic Bank Berhad
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import sys


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_DIR = Path("/path/to/input")
OUTPUT_DIR = Path("/path/to/output")
LIST_DIR = Path("/path/to/list")

# Input files
REPTDATE_PATH = INPUT_DIR / "REPTDATE.parquet"
CMM_PATH_TEMPLATE = INPUT_DIR / "CMM{}.parquet"
EQU_PATH_TEMPLATE = INPUT_DIR / "EQU{}.parquet"
TEMPLATE_PATH = INPUT_DIR / "TEMPL.txt"
WALK_PATH = INPUT_DIR / "WALK.txt"

# List files
INTRA_GROUP_PATH = LIST_DIR / "ICOF_MNI_INTRA_GROUP.parquet"
RELATED_PARTY_PATH = LIST_DIR / "ICOF_MNI_RELATED_PARTY.parquet"
EQU_INTRA_GROUP_PATH = LIST_DIR / "ICOF_EQU_INTRA_GROUP.parquet"
EQU_RELATED_PARTY_PATH = LIST_DIR / "ICOF_EQU_RELATED_PARTY.parquet"

# Output files
COF_OUTPUT_PATH = OUTPUT_DIR / "ICOF_OUTPUT.txt"
SFTP_SCRIPT_PATH = OUTPUT_DIR / "SFTP_SCRIPT.txt"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# FORMAT DEFINITIONS - ISLAMIC BANK
# ============================================================================

COFF1FMT = {
    '95315': '1.01I   ',
    '95317': '1.02I   ',
    '95312': '1.03I   ',
    '95313': '1.04I   ',
    '95810': '1.05I   ',
    '95820': '1.06I   ',
    '95830': '1.07I   ',
    '95840': '1.08I   ',
    '95850': '1.10I   ',
    '96317': '1.20II  ',
    '96313': '1.21II  ',
    '96810': '1.22II  ',
    '96820': '1.23II  ',
    '96830': '1.24II  ',
    '96840': '1.25II  ',
    '96850': '1.27II  ',
}

COFF2FMT = {
    '95315': '2.01I   ',
    '95317': '2.02I   ',
    '95312': '2.03I   ',
    '95313': '2.04I   ',
    '95810': '2.05I   ',
    '95820': '2.06I   ',
    '95830': '2.07I   ',
    '95840': '2.08I   ',
    '96317': '2.13II  ',
    '96313': '2.14II  ',
    '96810': '2.15II  ',
    '96820': '2.16II  ',
    '96830': '2.17II  ',
    '96840': '2.18II  ',
}

COFF3FMT = {
    '95315': '3.01I   ',
    '95317': '3.02I   ',
    '95312': '3.03I   ',
    '95313': '3.04I   ',
    '95830': '3.05I   ',
    '95840': '3.06I   ',
    '96317': '3.10II  ',
    '96313': '3.11II  ',
    '96830': '3.12II  ',
    '96840': '3.13II  ',
}

COFF4FMT = {
    '95315': '4.01    ',
    '95317': '4.02    ',
    '95312': '4.03    ',
    '95313': '4.04    ',
}

COFF5FMT = {
    '95315': '5.01    ',
    '95317': '5.02    ',
    '95313': '5.03    ',
    '95810': '5.05I   ',
    '95820': '5.05II  ',
    '95830': '5.05III ',
    '95840': '5.05IV  ',
}

GLCOFFMT = {
    'F143130': '1.12I   ',
    'F144111': '1.13I   ',
    'F147100': '1.13I   ',
    'F249120BP': '1.16I   ',
    'F142199C': '1.17I   ',
    'F142199D': '1.17I   ',
    'F142199E': '1.17I   ',
    'F142510FDA': '1.18I   ',
    'F142600A': '1.31II  ',
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def apply_format(value, format_dict):
    """Apply SAS-style format mapping."""
    return format_dict.get(value, '').ljust(8)


def format_number(value):
    """Format number with comma separators and 2 decimal places."""
    if value is None or (isinstance(value, float) and (value != value)):
        return ''
    try:
        return f"{float(value):,.2f}"
    except (ValueError, TypeError):
        return ''


# ============================================================================
# READ REPTDATE AND CALCULATE PARAMETERS
# ============================================================================

def read_reptdate():
    """Read reptdate and calculate reporting parameters."""
    print("Reading REPTDATE...")

    print("REPTDATE_PATH =", REPTDATE_PATH)
    print("Exists?", REPTDATE_PATH.exists())

    df = pl.read_parquet(REPTDATE_PATH)
    reptdate = df['REPTDATE'][0]

    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, "%Y-%m-%d").date()

    reptmon = f"{reptdate.month:02d}"
    fildt = reptdate.strftime("%d%m%y")
    rdate = reptdate.strftime("%d/%m/%Y")

    print(f"Report Date: {rdate}")
    print(f"Report Month: {reptmon}")

    return reptdate, reptmon, fildt, rdate


# ============================================================================
# READ TEMPLATE FILE
# ============================================================================

def read_template():
    """Read template file with item descriptions."""
    print("Reading template...")

    template_data = []
    recno = 1

    with open(TEMPLATE_PATH, 'r') as f:
        for line in f:
            if len(line) >= 10:
                item = line[0:8].strip().upper()
                idesc = line[9:129] if len(line) >= 129 else line[9:].rstrip('\n')
                template_data.append({
                    'ITEM': item,
                    'IDESC': idesc,
                    'RECNO': recno,
                    'AMOUNT': None,
                    'BUC1': None,
                    'BUC2': None,
                    'BUC3': None,
                    'BUC4': None,
                    'BUC5': None,
                    'BUC6': None,
                    'BUC7': None,
                })
                recno += 1

    df = pl.DataFrame(template_data)
    print(f"Template records: {len(df)}")
    return df


# ============================================================================
# READ AND PROCESS GL (WALK FILE)
# ============================================================================

def read_gl():
    """Read and process GL data from WALK file starting from line 2."""
    print("Reading GL data...")

    gl_data = []

    with open(WALK_PATH, 'r') as f:
        # Skip first line (FIRSTOBS=2)
        next(f)

        for line in f:
            if len(line) >= 62:
                set_id = line[1:20].strip()
                amount_str = line[41:61].strip()
                sign = line[61:62]

                try:
                    amount = float(amount_str.replace(',', ''))
                    if sign == ' ':
                        amount = -1 * amount
                except (ValueError, AttributeError):
                    amount = 0.0

                item = apply_format(set_id, GLCOFFMT)

                if item.strip():
                    buc5 = amount if set_id in ('F142199C', 'F142199D') else None
                    buc6 = amount if set_id in ('F144111', 'F147100') else None
                    buc7 = amount if buc5 is None and buc6 is None else None

                    gl_data.append({
                        'SET_ID': set_id,
                        'AMOUNT': amount,
                        'ITEM': item.strip(),
                        'BUC1': None,
                        'BUC2': None,
                        'BUC3': None,
                        'BUC4': None,
                        'BUC5': buc5,
                        'BUC6': buc6,
                        'BUC7': buc7,
                    })

    df = pl.DataFrame(gl_data)

    # Add PART1 and PART2
    df = df.with_columns([
        pl.col('ITEM').str.slice(0, 1).alias('PART1'),
        pl.col('ITEM').str.slice(4, 4).alias('PART2'),
    ])

    # Aggregate by ITEM
    df = df.group_by('ITEM').agg([
        pl.col('AMOUNT').sum(),
        pl.col('BUC1').sum(),
        pl.col('BUC2').sum(),
        pl.col('BUC3').sum(),
        pl.col('BUC4').sum(),
        pl.col('BUC5').sum(),
        pl.col('BUC6').sum(),
        pl.col('BUC7').sum(),
        pl.col('PART1').first(),
        pl.col('PART2').first(),
    ])

    print(f"GL records: {len(df)}")
    return df


# ============================================================================
# READ AND PROCESS COF DATA
# ============================================================================

def read_cof_data(reptmon):
    """Read CMM and EQU data and create initial COF dataset."""
    print("Reading COF data (CMM and EQU)...")

    cmm_path = Path(str(CMM_PATH_TEMPLATE).format(reptmon))
    equ_path = Path(str(EQU_PATH_TEMPLATE).format(reptmon))

    # Read CMM data
    df_cmm = pl.read_parquet(cmm_path)

    # Read EQU data and rename CUSTNO to CUSTEQNO
    df_equ = pl.read_parquet(equ_path)
    df_equ = df_equ.rename({'CUSTNO': 'CUSTEQNO'})

    # Combine datasets
    df = pl.concat([df_cmm, df_equ])

    # Add TAG = 1 (TOTAL LIABILITIES)
    df = df.with_columns(pl.lit(1).alias('TAG'))

    # Aggregate by CMMCODE and TAG
    df = df.group_by(['CMMCODE', 'TAG']).agg([
        pl.col('AMOUNT').sum()
    ])

    print(f"COF records (TAG=1): {len(df)}")
    return df


# ============================================================================
# READ LIST FILES AND CREATE EXCLUSION/INCLUSION LISTS
# ============================================================================

def read_list_files():
    """Read list files and create customer/IC lists."""
    print("Reading list files...")

    # INTRA GROUP - INTRAIC
    df_intra = pl.read_parquet(INTRA_GROUP_PATH)
    intraic = df_intra.filter(pl.col('BUSSREG') != '').select('BUSSREG')['BUSSREG'].to_list()

    # INTRA GROUP - INTRACUS
    intracus = df_intra.filter(pl.col('CUSTNO').is_not_null()).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - RELCUS
    df_related = pl.read_parquet(RELATED_PARTY_PATH)
    relcus = df_related.filter(pl.col('CUSTNO').is_not_null()).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - XRELCUS (exclusions)
    xrelcus = df_related.filter(
        pl.col('ICNEW').str.slice(0, 1) == '-'
    ).select('CUSTNO')['CUSTNO'].to_list()

    # RELATED PARTY - RELIC
    relic = df_related.filter(pl.col('ICNEW') != '').select('ICNEW')['ICNEW'].to_list()

    # EQU INTRA GROUP - INTRAEQ
    df_equ_intra = pl.read_parquet(EQU_INTRA_GROUP_PATH)
    intraeq = df_equ_intra.filter(pl.col('CUSTNO') != '').select('CUSTNO')['CUSTNO'].to_list()

    # EQU RELATED PARTY - RELEQ
    df_equ_related = pl.read_parquet(EQU_RELATED_PARTY_PATH)
    releq = df_equ_related.filter(pl.col('CUSTNO') != '').select('CUSTNO')['CUSTNO'].to_list()

    print(f"INTRAIC: {len(intraic)}, INTRACUS: {len(intracus)}")
    print(f"RELCUS: {len(relcus)}, XRELCUS: {len(xrelcus)}, RELIC: {len(relic)}")
    print(f"INTRAEQ: {len(intraeq)}, RELEQ: {len(releq)}")

    return {
        'INTRAIC': set(intraic),
        'INTRACUS': set(intracus),
        'RELCUS': set(relcus),
        'XRELCUS': set(xrelcus),
        'RELIC': set(relic),
        'INTRAEQ': set(intraeq),
        'RELEQ': set(releq),
    }


# ============================================================================
# CREATE COF23 (TAG 2 AND 3)
# ============================================================================

def create_cof23(reptmon, lists):
    """Create COF data with TAG 2 (INTRA GROUP) and TAG 3 (RELATED PARTY)."""
    print("Creating COF23 (TAG 2 and 3)...")

    cmm_path = Path(str(CMM_PATH_TEMPLATE).format(reptmon))
    equ_path = Path(str(EQU_PATH_TEMPLATE).format(reptmon))

    # Read CMM data
    df_cmm = pl.read_parquet(cmm_path)

    # Read EQU data and rename CUSTNO to CUSTEQNO
    df_equ = pl.read_parquet(equ_path)
    df_equ = df_equ.rename({'CUSTNO': 'CUSTEQNO'})

    # Combine datasets
    df = pl.concat([df_cmm, df_equ])

    # Create TAG based on conditions
    def assign_tag(row):
        custno = row.get('CUSTNO')
        custeqno = row.get('CUSTEQNO')
        newic = row.get('NEWIC', '')

        # TAG 3: RELATED PARTY
        if (custno in lists['RELCUS'] or
                newic in lists['RELIC'] or
                custeqno in lists['RELEQ']):
            return 3

        # TAG 2: INTRA GROUP
        if (custno in lists['INTRACUS'] or
                (newic in lists['INTRAIC'] and custno not in lists['XRELCUS']) or
                custeqno in lists['INTRAEQ']):
            return 2

        return None

    # Apply tag assignment
    tags = []
    for row in df.iter_rows(named=True):
        tags.append(assign_tag(row))

    df = df.with_columns(pl.Series('TAG', tags))

    # Filter only records with TAG 2 or 3
    df = df.filter(pl.col('TAG').is_in([2, 3]))

    # Aggregate by CMMCODE and TAG
    df = df.group_by(['CMMCODE', 'TAG']).agg([
        pl.col('AMOUNT').sum()
    ])

    print(f"COF23 records: {len(df)}")
    return df


# ============================================================================
# CREATE COF123 (COMBINE AND PROCESS)
# ============================================================================

def create_cof123(cof, cof23):
    """Combine COF and COF23, apply formatting and bucket allocation."""
    print("Creating COF123...")

    # Combine datasets
    df = pl.concat([cof, cof23])

    # Extract parts of CMMCODE
    df = df.with_columns([
        pl.col('CMMCODE').str.slice(0, 5).alias('BIC'),
        pl.col('CMMCODE').str.slice(5, 2).alias('CUST'),
        pl.col('CMMCODE').str.slice(7, 2).alias('REM'),
        pl.col('CMMCODE').str.slice(9, 2).alias('ECP'),
    ])

    # Apply format based on TAG
    def get_item(bic, tag):
        if tag == 1:
            return apply_format(bic, COFF1FMT)
        elif tag == 2:
            return apply_format(bic, COFF2FMT)
        elif tag == 3:
            return apply_format(bic, COFF3FMT)
        return ''

    items = []
    for row in df.iter_rows(named=True):
        items.append(get_item(row['BIC'], row['TAG']))

    df = df.with_columns(pl.Series('ITEM', items))

    # Filter only records with valid ITEM
    df = df.filter(pl.col('ITEM') != '')

    # Override REM for specific BICs
    df = df.with_columns(
        pl.when(pl.col('BIC').is_in(['95312', '95313', '96313', '9531X']))
        .then(pl.lit('07'))
        .otherwise(pl.col('REM'))
        .alias('REM')
    )

    # Round AMOUNT
    df = df.with_columns(
        pl.col('AMOUNT').round(2)
    )

    # Allocate AMOUNT to buckets based on REM
    df = df.with_columns([
        pl.when(pl.col('REM') == '01').then(pl.col('AMOUNT')).otherwise(None).alias('BUC1'),
        pl.when(pl.col('REM') == '02').then(pl.col('AMOUNT')).otherwise(None).alias('BUC2'),
        pl.when(pl.col('REM') == '03').then(pl.col('AMOUNT')).otherwise(None).alias('BUC3'),
        pl.when(pl.col('REM') == '04').then(pl.col('AMOUNT')).otherwise(None).alias('BUC4'),
        pl.when(pl.col('REM') == '05').then(pl.col('AMOUNT')).otherwise(None).alias('BUC5'),
        pl.when(pl.col('REM') == '06').then(pl.col('AMOUNT')).otherwise(None).alias('BUC6'),
        pl.when(pl.col('REM') == '07').then(pl.col('AMOUNT')).otherwise(None).alias('BUC7'),
    ])

    print(f"COF123 records: {len(df)}")
    return df


# ============================================================================
# CREATE COF45 (RETAIL/WHOLESALE BREAKDOWN)
# ============================================================================

def create_cof45(cof123):
    """Create COF45 for retail/wholesale funding breakdown."""
    print("Creating COF45...")

    # Filter TAG = 1 only
    df = cof123.filter(pl.col('TAG') == 1)

    # Apply format based on CUST
    def get_item_45(bic, cust, ecp):
        if cust == '08':
            item = apply_format(bic, COFF4FMT)
        else:
            item = apply_format(bic, COFF5FMT)

        # Operational adjustment
        if item.strip() == '5.03' and ecp == '01':
            item = '5.04    '

        return item

    items = []
    for row in df.iter_rows(named=True):
        items.append(get_item_45(row['BIC'], row['CUST'], row['ECP']))

    df = df.with_columns(pl.Series('ITEM', items))

    # Filter only records with valid ITEM
    df = df.filter(pl.col('ITEM') != '')

    print(f"COF45 records: {len(df)}")
    return df


# ============================================================================
# AGGREGATE AND CREATE SUMMARIES
# ============================================================================

def create_summaries(cof_combined):
    """Create item-level, subtotal, and total summaries."""
    print("Creating summaries...")

    # Add derived columns
    cof_combined = cof_combined.with_columns([
        pl.col('ITEM').str.slice(0, 1).alias('PART1'),
        pl.col('ITEM').str.slice(4, 4).alias('PART2'),
        pl.col('ITEM').str.slice(0, 4).alias('PREFIX'),
    ])

    # COFITEM - Item level summary
    cofitem = cof_combined.group_by('ITEM').agg([
        pl.col('AMOUNT').sum(),
        pl.col('BUC1').sum(),
        pl.col('BUC2').sum(),
        pl.col('BUC3').sum(),
        pl.col('BUC4').sum(),
        pl.col('BUC5').sum(),
        pl.col('BUC6').sum(),
        pl.col('BUC7').sum(),
    ])

    # COFSUBTOT - Subtotal by PART1 and PART2
    cofsubtot = cof_combined.group_by(['PART1', 'PART2']).agg([
        pl.col('AMOUNT').sum(),
        pl.col('BUC1').sum(),
        pl.col('BUC2').sum(),
        pl.col('BUC3').sum(),
        pl.col('BUC4').sum(),
        pl.col('BUC5').sum(),
        pl.col('BUC6').sum(),
        pl.col('BUC7').sum(),
    ])

    # COFTOT - Total by PART1
    coftot = cof_combined.group_by('PART1').agg([
        pl.col('AMOUNT').sum(),
        pl.col('BUC1').sum(),
        pl.col('BUC2').sum(),
        pl.col('BUC3').sum(),
        pl.col('BUC4').sum(),
        pl.col('BUC5').sum(),
        pl.col('BUC6').sum(),
        pl.col('BUC7').sum(),
    ])

    # COFSPCL - Special summary for operational/non-operational
    cofspcl = cof_combined.filter(
        pl.col('PREFIX').is_in(['5.04', '5.05'])
    ).group_by('PREFIX').agg([
        pl.col('AMOUNT').sum(),
        pl.col('BUC1').sum(),
        pl.col('BUC2').sum(),
        pl.col('BUC3').sum(),
        pl.col('BUC4').sum(),
        pl.col('BUC5').sum(),
        pl.col('BUC6').sum(),
        pl.col('BUC7').sum(),
    ])

    # Create ITEM for totals
    coftot = coftot.with_columns(
        (pl.col('PART1') + pl.lit('.99')).alias('ITEM')
    )

    cofsubtot = cofsubtot.with_columns(
        (pl.col('PART1') + pl.lit('.00') + pl.col('PART2')).alias('ITEM')
    )

    cofspcl = cofspcl.with_columns(
        pl.col('PREFIX').alias('ITEM')
    )

    # Combine all totals
    coftot_combined = pl.concat([
        coftot.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
        cofsubtot.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
        cofspcl.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
    ])

    print(f"COFITEM: {len(cofitem)}, COFTOT: {len(coftot_combined)}")
    return cofitem, coftot_combined


# ============================================================================
# GENERATE REPORT
# ============================================================================

def generate_report(template, cofitem, coftot_combined, rdate):
    """Generate final COF report with ASA carriage control."""
    print("Generating report...")

    # Merge template with item and total data
    df = template.join(cofitem, on='ITEM', how='left', suffix='_item')
    df = df.join(coftot_combined, on='ITEM', how='left', suffix='_tot')

    # Coalesce columns (prefer item values, then tot values, then template defaults)
    for col in ['AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']:
        item_col = f'{col}_item' if f'{col}_item' in df.columns else col
        tot_col = f'{col}_tot' if f'{col}_tot' in df.columns else None

        if tot_col and tot_col in df.columns:
            df = df.with_columns(
                pl.coalesce([item_col, tot_col, pl.col(col)]).alias(col)
            )
        elif item_col in df.columns:
            df = df.with_columns(
                pl.coalesce([item_col, pl.col(col)]).alias(col)
            )

    # Sort by RECNO
    df = df.sort('RECNO')

    # Write output with ASA carriage control
    delimiter = chr(0x05)

    with open(COF_OUTPUT_PATH, 'w') as f:
        # Header lines with ASA carriage control
        f.write(' PUBLIC ISLAMIC BANK BERHAD\n')
        f.write(f' LIQUIDITY COVERAGE RATIO (LCR) TABLE 4 AS AT {rdate}\n')
        f.write(' CONCENTRATION OF FUNDING\n')

        # Data rows
        for row in df.iter_rows(named=True):
            idesc = row['IDESC']

            # Print blank line if IDESC starts with non-blank
            if idesc and len(idesc) >= 2 and idesc[0:2].strip():
                f.write(' \n')

            # Print data line
            line_parts = [
                ' ',  # ASA carriage control (space = single spacing)
                idesc if idesc else '',
                delimiter,
                format_number(row.get('BUC1')),
                delimiter,
                format_number(row.get('BUC2')),
                delimiter,
                format_number(row.get('BUC3')),
                delimiter,
                format_number(row.get('BUC4')),
                delimiter,
                format_number(row.get('BUC5')),
                delimiter,
                format_number(row.get('BUC6')),
                delimiter,
                format_number(row.get('BUC7')),
                delimiter,
                format_number(row.get('AMOUNT')),
                delimiter,
            ]
            f.write(''.join(line_parts) + '\n')

            # Print column headers if IDESC starts with non-blank
            if idesc and len(idesc) >= 2 and idesc[0:2].strip():
                header_parts = [
                    ' ',  # ASA carriage control
                    '',
                    delimiter,
                    'Deposit Type',
                    delimiter,
                    'up to 1 week',
                    delimiter,
                    '> 1 wk - 1 mth',
                    delimiter,
                    '> 1 - 3 mths',
                    delimiter,
                    '> 3 - 6 mths',
                    delimiter,
                    '> 6 mths -  1 yr',
                    delimiter,
                    '> 1 year',
                    delimiter,
                    'No specific maturity',
                    delimiter,
                    'Total',
                    delimiter,
                ]
                f.write(''.join(header_parts) + '\n')

    print(f"Report written to {COF_OUTPUT_PATH}")


# ============================================================================
# GENERATE SFTP SCRIPT
# ============================================================================

def generate_sftp_script(fildt):
    """Generate SFTP script file."""
    print("Generating SFTP script...")

    with open(SFTP_SCRIPT_PATH, 'w') as f:
        f.write(f"put //SAP.PIBB.LCR.COF.TEXT  ICOF_{fildt}.XLS\n")

    print(f"SFTP script written to {SFTP_SCRIPT_PATH}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    try:
        print("=" * 80)
        print("Islamic Bank LCR Concentration of Funding Report Generator")
        print("=" * 80)

        # Read reptdate
        reptdate, reptmon, fildt, rdate = read_reptdate()

        # Read template
        template = read_template()

        # Read GL data
        gl = read_gl()

        # Read COF data
        cof = read_cof_data(reptmon)

        # Read list files
        lists = read_list_files()

        # Create COF23
        cof23 = create_cof23(reptmon, lists)

        # Create COF123
        cof123 = create_cof123(cof, cof23)

        # Create COF45
        cof45 = create_cof45(cof123)

        # Combine all COF data
        cof_combined = pl.concat([
            cof123.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
            cof45.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
            gl.select(['ITEM', 'AMOUNT', 'BUC1', 'BUC2', 'BUC3', 'BUC4', 'BUC5', 'BUC6', 'BUC7']),
        ])

        print(f"Total COF combined records: {len(cof_combined)}")

        # Create summaries
        cofitem, coftot_combined = create_summaries(cof_combined)

        # Generate report
        generate_report(template, cofitem, coftot_combined, rdate)

        # Generate SFTP script
        generate_sftp_script(fildt)

        print("=" * 80)
        print("Processing completed successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
