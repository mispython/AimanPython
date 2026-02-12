#!/usr/bin/env python3
"""
Program             : DALWPBBD
Invoke by program   : EIBWEEK1-3
PBB deposit manipulation extracted from SAP.PBB.MNITB

This program manipulates the extracted savings and current accounts database.
It applies formatting to codes, filters records based on account status and balance,
    and performs branch-level summarization.
"""

import duckdb
import polars as pl
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging

# ============================================================================
# CONFIGURATION AND SETUP
# ============================================================================

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
BASE_PATH = Path(__file__).parent
DATA_INPUT_PATH = BASE_PATH / "data" / "input"
DATA_OUTPUT_PATH = BASE_PATH / "data" / "output"

# Ensure output directories exist
DATA_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Input parquet file paths
DEPOSIT_SAVING_PATH = DATA_INPUT_PATH / "DEPOSIT_SAVING.parquet"
DEPOSIT_CURRENT_PATH = DATA_INPUT_PATH / "DEPOSIT_CURRENT.parquet"
CISDP_DEPOSIT_PATH = DATA_INPUT_PATH / "CISDP_DEPOSIT.parquet"

# Format mapping dictionaries (simulating SAS formats)
# These would typically come from PBBDPFMT include
SACUSTCD_FORMAT = {
    '001': '01', '002': '02', '003': '03', '004': '04', '005': '05',
    '010': '10', '020': '20', '030': '30', '040': '40', '050': '50',
    '077': '77', '078': '78', '081': '81', '095': '95'
}

STATECD_FORMAT = {
    'A': '1', 'B': '2', 'C': '3', 'D': '4', 'E': '5', 'F': '6'
}

SAPROD_FORMAT = {
    '100': '00100', '104': '00104', '105': '00105', '177': '00177',
    '200': '00200', '300': '00300'
}

SADENOM_FORMAT = {
    '100': 'A', '104': 'B', '105': 'C', '177': 'D',
    '200': 'E', '300': 'F'
}

CAPROD_FORMAT = {
    '104': '00104', '105': '00105', '400': '00400', '450': '00450'
}

CADENOM_FORMAT = {
    '104': 'B', '105': 'C', '400': 'D', '450': 'E'
}

DDCUSTCD_FORMAT = {
    '001': '01', '002': '02', '003': '03', '004': '04', '005': '05',
    '010': '10', '020': '20', '030': '30', '040': '40', '050': '50'
}

# ACE Products (from PBBDPFMT)
ACE_PRODUCTS = [177]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def apply_format(value: Optional[str], format_dict: Dict[str, str], default: str = '') -> str:
    """Apply SAS format mapping to a value."""
    if value is None:
        return default
    str_value = str(value).strip()
    return format_dict.get(str_value, default)


def safe_int(value) -> Optional[int]:
    """Safely convert value to integer."""
    try:
        if value is None:
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def process_savings_data(
        reptmon: str,
        nowk: str
) -> Tuple[Optional[pl.DataFrame], Dict[str, int]]:
    """
    Process savings account data.

    Equivalent to:
    DATA BNM.SAVG&REPTMON&NOWK &SAVG2;
    """
    logger.info(f"Processing savings data for REPTMON={reptmon}, NOWK={nowk}")

    try:
        # Read savings data using DuckDB
        conn = duckdb.connect(':memory:')

        # Load and process savings data
        query = f"""
        SELECT 
            BRANCH,
            PRODUCT,
            OPENIND,
            CUSTCODE,
            NAME,
            ACCTNO,
            CURBAL,
            INTPAYBL,
            COSTCTR,
            DNBFISME,
            CURCODE
        FROM read_parquet('{DEPOSIT_SAVING_PATH}')
        WHERE OPENIND NOT IN ('B', 'C', 'P')
        AND CURBAL >= 0
        """

        result = conn.execute(query).fetch_all()
        conn.close()

        if not result:
            logger.warning("No savings data found after filtering")
            return None, {'processed': 0}

        # Convert to Polars DataFrame
        df = pl.DataFrame(result, schema={
            'BRANCH': pl.Utf8,
            'PRODUCT': pl.Int32,
            'OPENIND': pl.Utf8,
            'CUSTCODE': pl.Utf8,
            'NAME': pl.Utf8,
            'ACCTNO': pl.Utf8,
            'CURBAL': pl.Float64,
            'INTPAYBL': pl.Float64,
            'COSTCTR': pl.Utf8,
            'DNBFISME': pl.Utf8,
            'CURCODE': pl.Utf8
        })

        # Apply formatting transformations
        df = df.with_columns([
            pl.col('CUSTCODE').map_elements(
                lambda x: apply_format(str(x), SACUSTCD_FORMAT),
                return_dtype=pl.Utf8
            ).alias('CUSTCD'),
            pl.col('BRANCH').map_elements(
                lambda x: apply_format(str(x), STATECD_FORMAT),
                return_dtype=pl.Utf8
            ).alias('STATECD'),
            pl.col('PRODUCT').cast(pl.Utf8).map_elements(
                lambda x: apply_format(x, SAPROD_FORMAT),
                return_dtype=pl.Utf8
            ).alias('PRODCD'),
            pl.col('PRODUCT').cast(pl.Utf8).map_elements(
                lambda x: apply_format(x, SADENOM_FORMAT),
                return_dtype=pl.Utf8
            ).alias('AMTIND')
        ])

        # Select final columns (SAVG2)
        df = df.select([
            'BRANCH', 'PRODUCT', 'CUSTCD', 'STATECD', 'PRODCD', 'NAME', 'ACCTNO',
            'CURBAL', 'INTPAYBL', 'AMTIND', 'COSTCTR', 'DNBFISME', 'CURCODE'
        ])

        output_file = DATA_OUTPUT_PATH / f"SAVG_{reptmon}_{nowk}.txt"
        df.write_csv(str(output_file), separator='\t')
        logger.info(f"Savings data written to {output_file}")

        return df, {'processed': len(df)}

    except Exception as e:
        logger.error(f"Error processing savings data: {e}")
        return None, {'error': str(e)}


def process_current_data(
        reptmon: str,
        nowk: str
) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame], Dict[str, int]]:
    """
    Process current account data.
    Splits output into CURN (current accounts) and FCY (foreign currency/facility).

    Equivalent to:
    DATA BNM.CURN&REPTMON&NOWK &CURN2
         BNM.FCY&REPTMON&NOWK &CURN2;
    """
    logger.info(f"Processing current data for REPTMON={reptmon}, NOWK={nowk}")

    try:
        # Read current account data using DuckDB
        conn = duckdb.connect(':memory:')

        query = f"""
        SELECT 
            BRANCH,
            PRODUCT,
            OPENIND,
            CUSTCODE,
            NAME,
            ACCTNO,
            CURBAL,
            INTPAYBL,
            ODINTACC,
            COSTCTR,
            SECTOR,
            DNBFISME,
            CURCODE,
            INTRATE,
            BILLERIND
        FROM read_parquet('{DEPOSIT_CURRENT_PATH}')
        WHERE OPENIND NOT IN ('B', 'C', 'P')
        AND CURBAL >= 0
        """

        result = conn.execute(query).fetch_all()
        conn.close()

        if not result:
            logger.warning("No current data found after filtering")
            return None, None, {'processed': 0}

        # Convert to Polars DataFrame
        df = pl.DataFrame(result, schema={
            'BRANCH': pl.Utf8,
            'PRODUCT': pl.Int32,
            'OPENIND': pl.Utf8,
            'CUSTCODE': pl.Utf8,
            'NAME': pl.Utf8,
            'ACCTNO': pl.Utf8,
            'CURBAL': pl.Float64,
            'INTPAYBL': pl.Float64,
            'ODINTACC': pl.Float64,
            'COSTCTR': pl.Utf8,
            'SECTOR': pl.Int32,
            'DNBFISME': pl.Utf8,
            'CURCODE': pl.Utf8,
            'INTRATE': pl.Float64,
            'BILLERIND': pl.Utf8
        })

        # Apply formatting transformations
        df = df.with_columns([
            pl.col('BRANCH').map_elements(
                lambda x: apply_format(str(x), STATECD_FORMAT),
                return_dtype=pl.Utf8
            ).alias('STATECD'),
            pl.col('PRODUCT').cast(pl.Utf8).map_elements(
                lambda x: apply_format(x, CAPROD_FORMAT),
                return_dtype=pl.Utf8
            ).alias('PRODCD'),
            pl.col('PRODUCT').cast(pl.Utf8).map_elements(
                lambda x: apply_format(x, CADENOM_FORMAT),
                return_dtype=pl.Utf8
            ).alias('AMTIND')
        ])

        # Add CUSTCD column with conditional logic for VOSTRO accounts
        def get_custcd(product: int, custcode: str) -> str:
            if product == 104:
                return '02'
            elif product == 105:
                return '81'
            else:
                return apply_format(custcode, DDCUSTCD_FORMAT)

        df = df.with_columns(
            pl.struct(['PRODUCT', 'CUSTCODE']).map_elements(
                lambda x: get_custcd(x['PRODUCT'], x['CUSTCODE']),
                return_dtype=pl.Utf8
            ).alias('CUSTCD')
        )

        # Process ACE products and sector adjustments
        curn_rows = []
        fcy_rows = []

        for row in df.iter_rows(named=True):
            row_dict = dict(row)

            if row_dict['PRODUCT'] in ACE_PRODUCTS:
                # ACE products: set INTPAYBL to 0 and output to CURN
                row_dict['INTPAYBL'] = 0
                row_dict['PRODCD'] = apply_format(str(row_dict['PRODUCT']), CAPROD_FORMAT)
                row_dict['AMTIND'] = apply_format(str(row_dict['PRODUCT']), CADENOM_FORMAT)
                curn_rows.append(row_dict)

            elif 400 <= row_dict['PRODUCT'] <= 444 or row_dict['PRODUCT'] in range(450, 455):
                # Facility products: adjust SECTOR and output to FCY
                custcd = row_dict['CUSTCD']
                sector = row_dict['SECTOR']

                if custcd in ['77', '78', '95']:
                    if sector in [4, 5]:
                        row_dict['SECTOR'] = 1
                    elif sector not in [1, 2, 3, 4, 5]:
                        row_dict['SECTOR'] = 1
                else:
                    if sector in [1, 2, 3]:
                        row_dict['SECTOR'] = 4
                    elif sector not in [1, 2, 3, 4, 5]:
                        row_dict['SECTOR'] = 4

                fcy_rows.append(row_dict)

            else:
                # Other products: output to CURN
                curn_rows.append(row_dict)

        # Convert results to Polars DataFrames
        curn_df = None
        fcy_df = None

        if curn_rows:
            curn_df = pl.DataFrame(curn_rows)
            # Select CURN2 columns
            curn_df = curn_df.select([
                'BRANCH', 'PRODUCT', 'CUSTCD', 'STATECD', 'PRODCD', 'NAME', 'ACCTNO',
                'CURBAL', 'INTPAYBL', 'AMTIND', 'ODINTACC', 'COSTCTR', 'SECTOR',
                'DNBFISME', 'CURCODE', 'INTRATE', 'BILLERIND'
            ])
            output_file = DATA_OUTPUT_PATH / f"CURN_{reptmon}_{nowk}.txt"
            curn_df.write_csv(str(output_file), separator='\t')
            logger.info(f"Current accounts data written to {output_file}")

        if fcy_rows:
            fcy_df = pl.DataFrame(fcy_rows)
            # Select CURN2 columns (same as CURN)
            fcy_df = fcy_df.select([
                'BRANCH', 'PRODUCT', 'CUSTCD', 'STATECD', 'PRODCD', 'NAME', 'ACCTNO',
                'CURBAL', 'INTPAYBL', 'AMTIND', 'ODINTACC', 'COSTCTR', 'SECTOR',
                'DNBFISME', 'CURCODE', 'INTRATE', 'BILLERIND'
            ])
            output_file = DATA_OUTPUT_PATH / f"FCY_{reptmon}_{nowk}.txt"
            fcy_df.write_csv(str(output_file), separator='\t')
            logger.info(f"Facility accounts data written to {output_file}")

        return curn_df, fcy_df, {'curn': len(curn_rows), 'fcy': len(fcy_rows)}

    except Exception as e:
        logger.error(f"Error processing current data: {e}")
        return None, None, {'error': str(e)}


def merge_fcy_with_cisdp(
        fcy_df: pl.DataFrame,
        reptmon: str,
        nowk: str
) -> Optional[pl.DataFrame]:
    """
    Merge FCY data with CISDP deposit data.

    Equivalent to:
    PROC SORT DATA=CISDP.DEPOSIT OUT=CISDP(KEEP=ACCTNO CUSTNO);
       BY ACCTNO;
    DATA BNM.FCY&REPTMON&NOWK;
       MERGE BNM.FCY&REPTMON&NOWK(IN=A) CISDP;
       BY ACCTNO;
       IF A;
    """
    logger.info(f"Merging FCY data with CISDP deposit data")

    try:
        if fcy_df is None or len(fcy_df) == 0:
            logger.warning("FCY dataframe is empty, skipping merge")
            return fcy_df

        # Read CISDP data
        cisdp_df = pl.read_parquet(str(CISDP_DEPOSIT_PATH)).select(['ACCTNO', 'CUSTNO'])

        # Merge FCY with CISDP
        merged_df = fcy_df.join(
            cisdp_df,
            on='ACCTNO',
            how='inner'
        )

        logger.info(f"Merged {len(merged_df)} records from FCY with CISDP")
        return merged_df

    except Exception as e:
        logger.error(f"Error merging FCY with CISDP: {e}")
        return fcy_df


def summarize_savings_data(
        savings_df: Optional[pl.DataFrame],
        reptmon: str,
        nowk: str
) -> Optional[pl.DataFrame]:
    """
    Summarize savings data at branch level.

    Equivalent to:
    PROC SUMMARY DATA=BNM.SAVG&REPTMON&NOWK NWAY;
    CLASS BRANCH STATECD PRODCD CUSTCD AMTIND;
    VAR CURBAL INTPAYBL;
    OUTPUT OUT=DEPT SUM=CURBAL INTPAYBL;
    """
    logger.info(f"Summarizing savings data at branch level")

    try:
        if savings_df is None or len(savings_df) == 0:
            logger.warning("Savings dataframe is empty, skipping summarization")
            return None

        dept_df = savings_df.group_by(
            ['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'AMTIND']
        ).agg([
            pl.col('CURBAL').sum(),
            pl.col('INTPAYBL').sum()
        ])

        logger.info(f"Savings summary created with {len(dept_df)} groups")
        return dept_df

    except Exception as e:
        logger.error(f"Error summarizing savings data: {e}")
        return None


def summarize_current_data(
        curn_df: Optional[pl.DataFrame],
        reptmon: str,
        nowk: str
) -> Optional[pl.DataFrame]:
    """
    Summarize current account data at branch level.

    Equivalent to:
    PROC SUMMARY DATA=BNM.CURN&REPTMON&NOWK NWAY MISSING;
    CLASS BRANCH STATECD PRODCD CUSTCD SECTOR AMTIND;
    VAR CURBAL INTPAYBL;
    OUTPUT OUT=DEPT SUM=CURBAL INTPAYBL;
    """
    logger.info(f"Summarizing current accounts data at branch level")

    try:
        if curn_df is None or len(curn_df) == 0:
            logger.warning("Current accounts dataframe is empty, skipping summarization")
            return None

        # Include MISSING in PROC SUMMARY by not filtering nulls
        dept_df = curn_df.group_by(
            ['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'SECTOR', 'AMTIND'],
            maintain_order=True
        ).agg([
            pl.col('CURBAL').sum(),
            pl.col('INTPAYBL').sum()
        ])

        logger.info(f"Current accounts summary created with {len(dept_df)} groups")
        return dept_df

    except Exception as e:
        logger.error(f"Error summarizing current accounts data: {e}")
        return None


def append_to_department_summary(
        savings_summary: Optional[pl.DataFrame],
        current_summary: Optional[pl.DataFrame],
        reptmon: str,
        nowk: str
) -> Optional[pl.DataFrame]:
    """
    Append savings and current account summaries to create final DEPT dataset.

    Equivalent to:
    PROC APPEND DATA=DEPT BASE=BNM.DEPT&REPTMON&NOWK;
    """
    logger.info(f"Combining savings and current summaries into department summary")

    try:
        dept_list = []

        if savings_summary is not None and len(savings_summary) > 0:
            # Ensure SECTOR column exists for savings (fill with null or default)
            if 'SECTOR' not in savings_summary.columns:
                savings_summary = savings_summary.with_columns(
                    pl.lit(None).cast(pl.Int32).alias('SECTOR')
                )
            dept_list.append(savings_summary)

        if current_summary is not None and len(current_summary) > 0:
            dept_list.append(current_summary)

        if not dept_list:
            logger.warning("No summaries to append")
            return None

        # Concatenate all summaries
        dept_df = pl.concat(dept_list, how='diagonal_relaxed')

        # Output to file
        output_file = DATA_OUTPUT_PATH / f"DEPT_{reptmon}_{nowk}.txt"
        dept_df.write_csv(str(output_file), separator='\t')
        logger.info(f"Department summary written to {output_file}")

        return dept_df

    except Exception as e:
        logger.error(f"Error combining summaries: {e}")
        return None


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main(reptmon: str = "202601", nowk: str = "W01"):
    """
    Main execution function.

    Args:
        reptmon: Report month in YYYYMM format
        nowk: Week number (e.g., W01, W02)
    """
    logger.info(f"Starting DALWPBBD processing for REPTMON={reptmon}, NOWK={nowk}")

    try:
        # Step 1: Process savings data
        savings_df, savings_stats = process_savings_data(reptmon, nowk)
        logger.info(f"Savings processing stats: {savings_stats}")

        # Step 2: Process current data
        curn_df, fcy_df, current_stats = process_current_data(reptmon, nowk)
        logger.info(f"Current processing stats: {current_stats}")

        # Step 3: Merge FCY with CISDP
        if fcy_df is not None:
            fcy_merged = merge_fcy_with_cisdp(fcy_df, reptmon, nowk)
            # Append FCY to CURN
            if fcy_merged is not None and len(fcy_merged) > 0 and curn_df is not None:
                curn_df = pl.concat([curn_df, fcy_merged.drop('CUSTNO')])
                logger.info(f"FCY data merged and appended to CURN")

        # Step 4: Summarize at branch level
        savings_summary = summarize_savings_data(savings_df, reptmon, nowk)
        current_summary = summarize_current_data(curn_df, reptmon, nowk)

        # Step 5: Append summaries
        final_dept = append_to_department_summary(
            savings_summary,
            current_summary,
            reptmon,
            nowk
        )

        logger.info(f"DALWPBBD processing completed successfully")
        if 'error' in savings_stats or 'error' in current_stats:
            return {'status': 'ERROR'}
        else:
            return {
            'status': 'SUCCESS',
            'savings_records': savings_stats.get('processed', 0),
            'current_records': current_stats.get('curn', 0) + current_stats.get('fcy', 0),
            'dept_summary_records': len(final_dept) if final_dept is not None else 0
        }

    except Exception as e:
        logger.error(f"Fatal error in main processing: {e}")
        return {'status': 'ERROR', 'error': str(e)}


if __name__ == "__main__":
    # Example execution with default parameters
    # For production use, parameters should be passed from job scheduling system
    result = main()
    logger.info(f"Final result: {result}")
