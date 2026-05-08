#!/usr/bin/env python3
"""
Dummy Data Generator for EIBHLNGR and EIIHLNGR
Generates shared input parquet files for both programs under their respective directories.
"""

import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date

# ============================================================================
# SEED FOR DETERMINISM
# ============================================================================
random.seed(42)

# ============================================================================
# OUTPUT BASE DIRECTORIES
# ============================================================================
BASE_DIRS = {
    "EIBHLNGR": Path(r"C:\Users\aiman\Desktop\SAS_Python_Migration\DUMMY\EIBHLNGR\input"),
    "EIIHLNGR": Path(r"C:\Users\aiman\Desktop\SAS_Python_Migration\DUMMY\EIIHLNGR\input"),
}

for d in BASE_DIRS.values():
    d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# SHARED CONSTANTS
# ============================================================================
REPORT_DATE = date(2024, 1, 31)   # January 2024 → REPTMON = "01"
REPTMON = f"{REPORT_DATE.month:02d}"

# CUSTCD values in scope (>= 80)
CUSTCD_VALUES = ["80", "81", "82", "83", "84", "85", "86", "87", "88",
                 "89", "90", "91", "92", "95", "96", "98", "99"]

# CGUARNAT values that qualify as guarantee
GUARNAT_QUALIFYING = ["01", "02", "03", "04", "05", "06", "07"]
GUARNAT_NON_QUALIFYING = ["08", "09", "10"]

PAIDIND_VALUES = ["A", "N", "D", "P", "C"]   # P and C are filtered unless EIR_ADJ set
PRODUCT_VALUES = ["TERMLN", "OVERDFT", "HOUSING", "HIRE", "PERSONAL"]

# ============================================================================
# HELPER: write parquet
# ============================================================================

def write_parquet(path: Path, table: pa.Table):
    pq.write_table(table, str(path))
    print(f"  Written: {path}")


# ============================================================================
# 1. loan_reptdate.parquet
# ============================================================================

def make_reptdate():
    df = pd.DataFrame({"REPTDATE": [REPORT_DATE]})
    return pa.Table.from_pandas(df, preserve_index=False)


# ============================================================================
# 2. loan_{REPTMON}4.parquet  (e.g. loan_014.parquet)
# ============================================================================

def make_loan_data(n=120):
    random.seed(42)
    rows = []
    acct_start = 1000000
    for i in range(n):
        acctno = acct_start + i
        noteno = f"N{acctno}"
        product = random.choice(PRODUCT_VALUES)
        custcd = random.choice(CUSTCD_VALUES)

        # Ensure some P/C records WITH EIR_ADJ to pass the filter
        paidind = random.choice(PAIDIND_VALUES)
        if paidind in ("P", "C"):
            eir_adj = round(random.uniform(0.001, 0.05), 6) if random.random() < 0.5 else None
        else:
            eir_adj = round(random.uniform(0.001, 0.05), 6) if random.random() < 0.3 else None

        bal_aft_eir = round(random.uniform(5000.0, 5_000_000.0), 2)

        rows.append({
            "ACCTNO":      acctno,
            "NOTENO":      noteno,
            "PRODUCT":     product,
            "CUSTCD":      custcd,
            "PAIDIND":     paidind,
            "EIR_ADJ":     eir_adj,
            "BAL_AFT_EIR": bal_aft_eir,
        })

    schema = pa.schema([
        pa.field("ACCTNO",      pa.int64()),
        pa.field("NOTENO",      pa.string()),
        pa.field("PRODUCT",     pa.string()),
        pa.field("CUSTCD",      pa.string()),
        pa.field("PAIDIND",     pa.string()),
        pa.field("EIR_ADJ",     pa.float64()),
        pa.field("BAL_AFT_EIR", pa.float64()),
    ])
    df = pd.DataFrame(rows)
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


# ============================================================================
# 3. coll_collater.parquet
# ============================================================================

def make_coll_collater(loan_table: pa.Table, n_extra=20):
    random.seed(42)
    loan_df = loan_table.to_pandas()
    rows = []

    # Attach collateral/guarantee to ~60% of loan accounts
    for _, lrow in loan_df.iterrows():
        if random.random() < 0.6:
            cguarnat = random.choice(GUARNAT_QUALIFYING + GUARNAT_NON_QUALIFYING)
            cdolarv = round(random.uniform(1000.0, 3_000_000.0), 2) if random.random() > 0.1 else 0.0
            rows.append({
                "ACCTNO":   lrow["ACCTNO"],
                "NOTENO":   lrow["NOTENO"],
                "CCOLLNO":  f"C{lrow['ACCTNO']}{random.randint(1,9)}",
                "CDOLARV":  cdolarv,
                "CGUARNAT": cguarnat,
            })

    # A few extra rows with non-matching accounts (won't join)
    for i in range(n_extra):
        fake_acct = 9000000 + i
        rows.append({
            "ACCTNO":   fake_acct,
            "NOTENO":   f"N{fake_acct}",
            "CCOLLNO":  f"C{fake_acct}1",
            "CDOLARV":  round(random.uniform(500.0, 50000.0), 2),
            "CGUARNAT": random.choice(GUARNAT_QUALIFYING),
        })

    schema = pa.schema([
        pa.field("ACCTNO",   pa.int64()),
        pa.field("NOTENO",   pa.string()),
        pa.field("CCOLLNO",  pa.string()),
        pa.field("CDOLARV",  pa.float64()),
        pa.field("CGUARNAT", pa.string()),
    ])
    df = pd.DataFrame(rows)
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


# ============================================================================
# MAIN: generate and write files for both programs
# ============================================================================

def main():
    print("Generating shared dummy data for EIBHLNGR and EIIHLNGR...")

    reptdate_table = make_reptdate()
    loan_table     = make_loan_data(n=120)
    coll_table     = make_coll_collater(loan_table, n_extra=20)

    loan_filename = f"loan_{REPTMON}4.parquet"

    for prog, base in BASE_DIRS.items():
        print(f"\n[{prog}]")
        write_parquet(base / "loan_reptdate.parquet", reptdate_table)
        write_parquet(base / loan_filename,           loan_table)
        write_parquet(base / "coll_collater.parquet", coll_table)

    print("\nDone. All dummy files written.")


if __name__ == "__main__":
    main()
