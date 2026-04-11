#!/usr/bin/env python3
"""
Program : EIIMLN03.py
Purpose : WEIGHTED AVERAGE LENDING RATE REPORT AND FLAT FILE OUTPUT FOR SRS
"""

import duckdb
import polars as pl
from datetime import datetime
from pathlib import Path

# ============================================================================
# PATHS
# ============================================================================
INPUT_DIR         = Path("input")
OUTPUT_DIR        = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTDATE_PARQUET  = INPUT_DIR / "bnm_reptdate.parquet"
SDESC_PARQUET     = INPUT_DIR / "bnm_sdesc.parquet"
GP3_PARQUET       = INPUT_DIR / "odgp3_gp3.parquet"
# BNM.LOAN&REPTMON&NOWK resolved at runtime
LOAN_PARQUET_TPL  = INPUT_DIR / "bnm_loan{reptmon}{nowk}.parquet"

OUTPUT_RPT        = OUTPUT_DIR / "EIIMLN03.txt"
OUTPUT_FLAT       = OUTPUT_DIR / "M4LOAN.txt"

PAGE_LENGTH = 60

# ============================================================================
# FORMAT
# ============================================================================

LNFMT_MAP = {
    'P1': 'PRESCRIBED RATE (HOUSING LOANS)',
    'P2': 'PRESCRIBED RATE (BNM FUNDED LOANS)',
    'P3': 'NON-PRESCRIBED RATE (HOUSING LOANS)',
    'P4': 'NON-PRESCRIBED RATE (OTHER LOANS)',
}

# LN03FMT from PBBLNFMT
from PBBLNFMT import format_ln03fmt

# ============================================================================
# LOAD REPTDATE
# ============================================================================

def load_reptdate():
    con = duckdb.connect()
    row = con.execute(f"SELECT reptdate FROM read_parquet('{REPTDATE_PARQUET}') LIMIT 1").fetchone()
    reptdate = row[0]
    if isinstance(reptdate, datetime):
        reptdate = reptdate.date()
    day = reptdate.day
    if day == 8:   nowk = '1'
    elif day == 15: nowk = '2'
    elif day == 22: nowk = '3'
    else:           nowk = '4'
    reptyear = str(reptdate.year)
    reptmon  = f"{reptdate.month:02d}"
    reptday  = f"{reptdate.day:02d}"
    rdate    = reptdate.strftime("%d/%m/%y")
    return reptdate, nowk, reptyear, reptmon, reptday, rdate


def load_sdesc():
    con = duckdb.connect()
    row = con.execute(f"SELECT sdesc FROM read_parquet('{SDESC_PARQUET}') LIMIT 1").fetchone()
    return str(row[0])[:26] if row else ''


# ============================================================================
# LOAD GP3
# ============================================================================

def load_gp3() -> pl.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{GP3_PARQUET}')").pl()

    def compute_loanstat(riskcode):
        try:
            riskrte = int(str(riskcode).strip()[:1])
        except Exception:
            riskrte = 0
        return 1 if riskrte < 1 else None

    loanstat_vals = [compute_loanstat(r) for r in df['RISKCODE'].to_list()]
    df = df.with_columns(pl.Series('LOANSTAT', loanstat_vals))
    return df.select([c for c in ['ACCTNO', 'LOANSTAT'] if c in df.columns])


# ============================================================================
# LOAD AND PROCESS LOAN
# ============================================================================

def load_loan(reptmon: str, nowk: str) -> pl.DataFrame:
    loan_path = Path(str(LOAN_PARQUET_TPL).replace('{reptmon}', reptmon).replace('{nowk}', nowk))
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{loan_path}')").pl()
    # IF PRODUCT IN (124,145) AND PRODCD='54120' THEN DELETE
    df = df.filter(
        ~((pl.col('PRODUCT').is_in([124, 145])) & (pl.col('PRODCD') == '54120'))
    )
    return df


def build_loan(loan_raw: pl.DataFrame, gp3: pl.DataFrame) -> pl.DataFrame:
    loan = loan_raw.sort('ACCTNO')
    gp3s = gp3.sort('ACCTNO')

    merged = loan.join(gp3s, on='ACCTNO', how='left', suffix='_GP3')

    # Resolve LOANSTAT: GP3 value overrides if present
    if 'LOANSTAT_GP3' in merged.columns:
        merged = merged.with_columns(
            pl.when(pl.col('LOANSTAT_GP3').is_not_null())
              .then(pl.col('LOANSTAT_GP3'))
              .otherwise(pl.col('LOANSTAT') if 'LOANSTAT' in merged.columns else pl.lit(None))
              .alias('LOANSTAT')
        ).drop('LOANSTAT_GP3')
    elif 'LOANSTAT' not in merged.columns:
        merged = merged.with_columns(pl.lit(None).cast(pl.Int64).alias('LOANSTAT'))

    merged = merged.with_columns(pl.col('BRANCH').alias('BRHNO'))

    # IF PRODCD='34111' THEN DELETE
    merged = merged.filter(pl.col('PRODCD') != '34111')
    # IF SUBSTR(PRODCD,1,2) = '34'
    merged = merged.filter(pl.col('PRODCD').str.slice(0, 2) == '34')

    rows = []
    for r in merged.iter_rows(named=True):
        acctype  = r.get('ACCTYPE', '') or ''
        product  = r.get('PRODUCT') or 0
        prodcd   = r.get('PRODCD', '') or ''
        intrate  = r.get('INTRATE', 0) or 0
        balance  = r.get('BALANCE', 0) or 0
        brhno    = r.get('BRHNO')
        loanstat = r.get('LOANSTAT')
        spread   = r.get('SPREAD', 0) or 0
        census   = r.get('CENSUS')
        loantyp  = None
        delete   = False

        if acctype == 'OD':
            loantyp = 'P4'
            if product in (93, 162):
                delete = True
            elif product == 119:
                loantyp = 'P1'
            elif product in (120, 137, 138, 154, 155, 192, 193, 194, 195):
                loantyp = 'P3'
            elif product in (73, 187, 188, 47, 48, 49, 17, 14):
                loantyp = 'P2'
            # RISKRTE check for OD
            try:
                riskrte = int(str(r.get('RISKCODE', '0')).strip()[:1])
            except Exception:
                riskrte = 0
            if riskrte < 1:
                loanstat = 1

        elif acctype == 'LN':
            if product in (225, 226):
                if intrate <= 9 or spread <= 1.75:
                    loantyp = 'P1'
                else:
                    loantyp = 'P3'
            else:
                loantyp = format_ln03fmt(product)

            delete_products = set(
                [668, 669, 670, 672, 673, 674, 675, 690, 671, 676, 677] +
                list(range(851, 861)) +
                list(range(691, 696))
            )
            if product in delete_products:
                delete = True

            if product == 169 and census in (169.01, 169.02, 169.03, 169.04):
                loantyp = 'P2'

        if delete:
            continue
        if loantyp == 'SL':
            continue
        if loantyp is None:
            continue

        rows.append({
            'LOANTYP':  loantyp,
            'PRODCD':   prodcd,
            'LOANSTAT': loanstat,
            'INTRATE':  intrate,
            'BALANCE':  balance,
            'BRHNO':    brhno,
        })

    return pl.DataFrame(rows) if rows else pl.DataFrame(
        schema={'LOANTYP': pl.Utf8, 'PRODCD': pl.Utf8, 'LOANSTAT': pl.Int64,
                'INTRATE': pl.Float64, 'BALANCE': pl.Float64, 'BRHNO': pl.Int64}
    )


# ============================================================================
# SUMMARISE
# ============================================================================

def summarise(df: pl.DataFrame, group_cols: list, var_cols: list) -> pl.DataFrame:
    if df.is_empty():
        return df
    existing = [c for c in group_cols if c in df.columns]
    return df.group_by(existing).agg([pl.col(c).sum() for c in var_cols if c in df.columns])


# ============================================================================
# REPORT PRINTING (PROC PRINT equivalent)
# ============================================================================

def fmt_comma18(val):
    if val is None:
        return ' ' * 18
    return f"{val:>18,.2f}"


def print_section(alm: pl.DataFrame, title1: str, title2: str, title3: str,
                  title4: str, lines: list, page_ctr: list):
    """Render a PROC PRINT BY LOANTYP with SUMBY and SUM."""
    if alm.is_empty():
        return

    def page_header():
        lines.append('1' + title1)
        if title2:
            lines.append(' ' + title2)
        lines.append(' ' + title3)
        if title4:
            lines.append(' ' + title4)
        lines.append(' ')
        lines.append(f" {'LOANTYP':<35} {'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}")
        lines.append(' ' + '-' * 85)
        page_ctr[0] = 7

    page_header()

    alm_sorted = alm.sort(['LOANTYP', 'INTRATE'])
    prev_loantyp = None
    tot_balance = 0.0
    tot_product = 0.0
    grand_balance = 0.0
    grand_product = 0.0

    for r in alm_sorted.iter_rows(named=True):
        lt  = r.get('LOANTYP', '')
        ir  = r.get('INTRATE', 0) or 0
        bal = r.get('BALANCE', 0) or 0
        prd = r.get('PRODUCT', 0) or 0

        if page_ctr[0] >= PAGE_LENGTH:
            page_header()

        if prev_loantyp is not None and lt != prev_loantyp:
            lines.append(' ' + '-' * 85)
            lines.append(
                f" {'LOANTYP TOTAL':<35} {'':>10} {fmt_comma18(tot_balance)} {fmt_comma18(tot_product)}"
            )
            lines.append(' ')
            tot_balance = 0.0
            tot_product = 0.0
            page_ctr[0] += 3

        if lt != prev_loantyp:
            lnlabel = LNFMT_MAP.get(lt, lt)
            lines.append(f" {lnlabel}")
            page_ctr[0] += 1
            prev_loantyp = lt

        lines.append(f" {'':35} {ir:>10.4f} {fmt_comma18(bal)} {fmt_comma18(prd)}")
        page_ctr[0] += 1
        tot_balance  += bal
        tot_product  += prd
        grand_balance += bal
        grand_product += prd

    if prev_loantyp is not None:
        lines.append(' ' + '-' * 85)
        lines.append(
            f" {'LOANTYP TOTAL':<35} {'':>10} {fmt_comma18(tot_balance)} {fmt_comma18(tot_product)}"
        )
        lines.append(' ')
        lines.append(' ' + '=' * 85)
        lines.append(
            f" {'GRAND TOTAL':<35} {'':>10} {fmt_comma18(grand_balance)} {fmt_comma18(grand_product)}"
        )
        lines.append(' ' + '=' * 85)


def print_section_no_by(alm: pl.DataFrame, title1: str, title3: str,
                         title4: str, lines: list, page_ctr: list):
    """Render a PROC PRINT without BY (just INTRATE, BALANCE, PRODUCT with SUM)."""
    if alm.is_empty():
        return

    lines.append('1' + title1)
    lines.append(' ' + title3)
    if title4:
        lines.append(' ' + title4)
    lines.append(' ')
    lines.append(f" {'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}")
    lines.append(' ' + '-' * 50)
    page_ctr[0] = 6

    alm_sorted = alm.sort('INTRATE')
    grand_balance = 0.0
    grand_product = 0.0

    for r in alm_sorted.iter_rows(named=True):
        if page_ctr[0] >= PAGE_LENGTH:
            lines.append('1' + title1)
            lines.append(' ' + title3)
            if title4:
                lines.append(' ' + title4)
            lines.append(' ')
            lines.append(f" {'INTRATE':>10} {'BALANCE':>18} {'PRODUCT':>18}")
            lines.append(' ' + '-' * 50)
            page_ctr[0] = 6

        ir  = r.get('INTRATE', 0) or 0
        bal = r.get('BALANCE', 0) or 0
        prd = r.get('PRODUCT', 0) or 0
        lines.append(f" {ir:>10.4f} {fmt_comma18(bal)} {fmt_comma18(prd)}")
        page_ctr[0] += 1
        grand_balance += bal
        grand_product += prd

    lines.append(' ' + '=' * 50)
    lines.append(f" {'TOTAL':<10} {fmt_comma18(grand_balance)} {fmt_comma18(grand_product)}")
    lines.append(' ' + '=' * 50)


# ============================================================================
# FLAT FILE OUTPUT
# ============================================================================

def write_flat_file(alm: pl.DataFrame, alm1: pl.DataFrame, loan_br: pl.DataFrame,
                    reptyear: str, reptmon: str, reptday: str):
    lines = []
    first = True

    def write_header():
        lines.append(f"{reptyear}{reptmon}{reptday}")

    write_header()

    for r in alm.iter_rows(named=True):
        intrate = round((r.get('INTRATE', 0) or 0) * 100)
        balance = round((r.get('BALANCE', 0) or 0) * 100)
        lines.append(f"001M4 000001{intrate:04d}{balance:015d}")

    for r in alm1.iter_rows(named=True):
        intrate = round((r.get('INTRATE', 0) or 0) * 100)
        balance = round((r.get('BALANCE', 0) or 0) * 100)
        lines.append(f"001M4 000004{intrate:04d}{balance:015d}")

    for r in loan_br.iter_rows(named=True):
        brhno   = r.get('BRHNO', 0) or 0
        intrate = round((r.get('INTRATE', 0) or 0) * 100)
        balance = round((r.get('BALANCE', 0) or 0) * 100)
        lines.append(f"{brhno:03d}M4 000009{intrate:04d}{balance:015d}")

    lines.append('EOF')

    with open(OUTPUT_FLAT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f"Flat file written to {OUTPUT_FLAT}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    reptdate, nowk, reptyear, reptmon, reptday, rdate = load_reptdate()
    sdesc = load_sdesc()
    gp3   = load_gp3()
    loan_raw = load_loan(reptmon, nowk)
    loan = build_loan(loan_raw, gp3)

    report_lines = []
    page_ctr = [0]

    title1 = 'REPORT ID : EIIMLN03'
    title2 = sdesc

    # ---- Section 1: Exclude penalty/litigation (LOANSTAT=1) ----
    loan_stat1 = loan.filter(pl.col('LOANSTAT') == 1)
    alm = summarise(loan_stat1, ['LOANTYP', 'INTRATE'], ['BALANCE'])
    alm = alm.with_columns((pl.col('INTRATE') * pl.col('BALANCE')).alias('PRODUCT'))

    print_section(
        alm, title1, title2,
        f'WEIGHTED AVERAGE LENDING RATE AS AT {rdate}',
        '', report_lines, page_ctr
    )

    # ---- Section 2: Prescribed rates incl. penalty/litigation ----
    loan_pres = loan.filter(pl.col('LOANTYP').is_in(['P1', 'P2']))
    alm2 = summarise(loan_pres, ['LOANTYP', 'INTRATE'], ['BALANCE'])
    alm2 = alm2.with_columns((pl.col('INTRATE') * pl.col('BALANCE')).alias('PRODUCT'))

    print_section(
        alm2, title1, title2,
        f'WEIGHTED AVERAGE LENDING RATE (PRESCRIBED) AS AT {rdate}',
        '(INCLUDES ACCOUNTS WITH PENALTY RATES & UNDER LITIGATION)',
        report_lines, page_ctr
    )

    # ---- Section 3: Prescribed by INTRATE only ----
    alm3 = summarise(alm2, ['INTRATE'], ['BALANCE', 'PRODUCT'])

    print_section_no_by(
        alm3, title1,
        f'WEIGHTED AVERAGE LENDING RATE (PRESCRIBED) AS AT {rdate}',
        '(INCLUDES ACCOUNTS WITH PENALTY RATES & UNDER LITIGATION)',
        report_lines, page_ctr
    )

    with open(OUTPUT_RPT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines) + '\n')
    print(f"Report written to {OUTPUT_RPT}")

    # ---- Flat file: SRS output ----
    # ACC TYPE 1: LOANSTAT=1, LOANTYP IN P1/P2
    alm_srs1 = summarise(
        loan.filter((pl.col('LOANSTAT') == 1) & pl.col('LOANTYP').is_in(['P1', 'P2'])),
        ['INTRATE'], ['BALANCE']
    )
    # ACC TYPE 4: LOANSTAT=1, PRODCD != '34111'
    alm_srs4 = summarise(
        loan.filter((pl.col('LOANSTAT') == 1) & (pl.col('PRODCD') != '34111')),
        ['INTRATE'], ['BALANCE']
    )
    # ACC TYPE 9: by BRHNO and INTRATE (all LOAN, no additional filter)
    loan_br = summarise(loan, ['BRHNO', 'INTRATE'], ['BALANCE'])

    write_flat_file(alm_srs1, alm_srs4, loan_br, reptyear, reptmon, reptday)


if __name__ == '__main__':
    main()
