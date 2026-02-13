#!/usr/bin/env python3
"""
Program: LALMPIBP

This script reproduces the core SAS transformations and output artifacts:
- Builds BNM.LALM{REPTMON}{NOWK} aggregates from parquet sources.
- Writes detail dataset in parquet.
- Writes PROC PRINT-style report with ASA carriage control characters.
- Produces LNAGING dataset/report sections.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Path / environment setup (defined up-front)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REPTMON = os.getenv("REPTMON", "")
NOWK = os.getenv("NOWK", "")
SDESC = os.getenv("SDESC", "")
RDATE = os.getenv("RDATE", "")

LOAN_FILE = Path(os.getenv("LOAN_FILE", str(INPUT_DIR / f"LOAN{REPTMON}{NOWK}.parquet")))
ULOAN_FILE = Path(os.getenv("ULOAN_FILE", str(INPUT_DIR / f"ULOAN{REPTMON}{NOWK}.parquet")))
LNCOMM_FILE = Path(os.getenv("LNCOMM_FILE", str(INPUT_DIR / "LNCOMM.parquet")))
LNNOTE_FILE = Path(os.getenv("LNNOTE_FILE", str(INPUT_DIR / "LNNOTE.parquet")))
CURRENT_FILE = Path(os.getenv("CURRENT_FILE", str(INPUT_DIR / "CURRENT.parquet")))

OUTPUT_LALM = Path(os.getenv("OUTPUT_LALM", str(OUTPUT_DIR / f"LALM{REPTMON}{NOWK}.parquet")))
OUTPUT_REPORT = Path(os.getenv("OUTPUT_REPORT", str(OUTPUT_DIR / f"LALMPIBP_{REPTMON}{NOWK}.txt")))
OUTPUT_LNAGING = Path(os.getenv("OUTPUT_LNAGING", str(OUTPUT_DIR / f"LNAGING_{REPTMON}{NOWK}.parquet")))
OUTPUT_AGING_REPORT = Path(os.getenv("OUTPUT_AGING_REPORT", str(OUTPUT_DIR / f"LALMPBBP_{REPTMON}{NOWK}.txt")))

ASA_NEW_PAGE = "1"
ASA_SPACE = " "
PAGE_LENGTH = 60


def _duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    return con

print("LOAN_FILE:", LOAN_FILE)
print("Exists?", LOAN_FILE.exists(), "\n")

def _load_parquet(con: duckdb.DuckDBPyConnection, name: str, path: Path) -> None:
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path.as_posix()}')")


def _append_rows(store: list[pl.DataFrame], df: pl.DataFrame) -> None:
    if df.height > 0:
        store.append(df.select(["BNMCODE", "AMTIND", "AMOUNT"]))


def _sector_bucket_expr(sector_col: str) -> str:
    return f"""
    CASE
      WHEN {sector_col} IN ('0410','0420','0430','9999') THEN {sector_col}
      WHEN SUBSTR({sector_col},1,2)='01' THEN '0100'
      WHEN SUBSTR({sector_col},1,2)='02' THEN '0200'
      WHEN SUBSTR({sector_col},1,3)='031' THEN '0310'
      WHEN SUBSTR({sector_col},1,3)='032' THEN '0320'
      WHEN SUBSTR({sector_col},1,1)='1' THEN '1000'
      WHEN SUBSTR({sector_col},1,1)='2' THEN '2000'
      WHEN SUBSTR({sector_col},1,1)='3' THEN '3000'
      WHEN SUBSTR({sector_col},1,1)='4' THEN '4000'
      WHEN SUBSTR({sector_col},1,1)='5' THEN {sector_col}
      WHEN SUBSTR({sector_col},1,2)='61' THEN '6100'
      WHEN SUBSTR({sector_col},1,2)='62' THEN '6200'
      WHEN SUBSTR({sector_col},1,2)='63' THEN '6300'
      WHEN SUBSTR({sector_col},1,1)='7' THEN '7000'
      WHEN SUBSTR({sector_col},1,2)='81' THEN '8100'
      WHEN SUBSTR({sector_col},1,2)='82' THEN '8200'
      WHEN SUBSTR({sector_col},1,3)='831' THEN '8310'
      WHEN SUBSTR({sector_col},1,3)='832' THEN '8320'
      WHEN SUBSTR({sector_col},1,3)='833' THEN '8330'
      WHEN SUBSTR({sector_col},1,2) IN ('90','91','92','93','94','95','96','97','98') THEN '9000'
      ELSE '9999'
    END
    """


def build_lalm() -> pl.DataFrame:
    con = _duck()
    _load_parquet(con, "loan_raw", LOAN_FILE)
    _load_parquet(con, "uloan_raw", ULOAN_FILE)
    _load_parquet(con, "lncomm_raw", LNCOMM_FILE)
    _load_parquet(con, "lnnote_raw", LNNOTE_FILE)
    _load_parquet(con, "current_raw", CURRENT_FILE)

    con.execute(
        """
        CREATE OR REPLACE VIEW loan AS
        SELECT *
        FROM loan_raw
        WHERE PAIDIND NOT IN ('P','C') OR EIR_ADJ IS NOT NULL
        """
    )
    con.execute("CREATE OR REPLACE VIEW m_lncomm AS SELECT ACCTNO, COMMNO, CUSEDAMT, CCURAMT FROM lncomm_raw")
    con.execute("CREATE OR REPLACE VIEW m_lnnote AS SELECT ACCTNO, COMMNO, CJFEE, BALANCE FROM lnnote_raw")

    out_parts: list[pl.DataFrame] = []

    # Non-performing loans (NPL)
    npl = pl.from_arrow(
        con.execute(
            """
            SELECT RISKCD || '00000000Y' AS BNMCODE, AMTIND, SUM(BAL_AFT_EIR) AS AMOUNT
            FROM loan
            WHERE SUBSTR(PRODCD,1,2)='34' AND COALESCE(RISKCD,'') <> ''
            GROUP BY RISKCD, AMTIND
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, npl)

    # NPL by customer and sectorial code (core mapping)
    npl2 = pl.from_arrow(
        con.execute(
            f"""
            WITH s AS (
              SELECT
                AMTIND,
                SECTORCD,
                CUSTCD,
                SUM(BAL_AFT_EIR) AS AMOUNT
              FROM loan
              WHERE SUBSTR(PRODCD,1,2)='34' AND COALESCE(RISKCD,'') <> ''
              GROUP BY AMTIND, SECTORCD, CUSTCD
            ),
            x AS (
              SELECT AMTIND, AMOUNT,
                     '349000000' || ({_sector_bucket_expr('SECTORCD')}) || 'Y' AS BNMCODE
              FROM s
            ),
            y AS (
              SELECT AMTIND, AMOUNT,
                     CASE
                       WHEN CUSTCD IN ('61','66') THEN '349006100' || ({_sector_bucket_expr('SECTORCD')}) || 'Y'
                       WHEN CUSTCD='77' THEN
                         CASE
                           WHEN SECTORCD IN ('0410','0420','0430') THEN '34900' || CUSTCD || '00' || SECTORCD || 'Y'
                           WHEN SUBSTR(SECTORCD,1,2)='01' THEN '34900' || CUSTCD || '000100Y'
                           WHEN SUBSTR(SECTORCD,1,2)='02' THEN '34900' || CUSTCD || '000200Y'
                           WHEN SUBSTR(SECTORCD,1,3)='031' THEN '34900' || CUSTCD || '000310Y'
                           WHEN SUBSTR(SECTORCD,1,3)='032' THEN '34900' || CUSTCD || '000320Y'
                           ELSE '34900' || CUSTCD || '009999Y'
                         END
                     END AS BNMCODE
              FROM s
              WHERE CUSTCD IN ('61','66','77')
            )
            SELECT BNMCODE, AMTIND, AMOUNT FROM x
            UNION ALL
            SELECT BNMCODE, AMTIND, AMOUNT FROM y WHERE BNMCODE IS NOT NULL
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, npl2)

    # RM loans
    rm_prod = pl.from_arrow(
        con.execute(
            """
            SELECT PRODCD || '00000000Y' AS BNMCODE, AMTIND, SUM(BAL_AFT_EIR) AS AMOUNT
            FROM loan
            WHERE SUBSTR(PRODCD,1,3) IN ('341','342','343','344')
              AND PRODCD IN ('34190','34230','34299')
            GROUP BY PRODCD, AMTIND
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, rm_prod)

    # RM loans overdraft by customer code (per PRODCD code path)
    rm_od = pl.from_arrow(
        con.execute(
            """
            SELECT CASE WHEN PRODCD='34240' THEN '3424000000000Y' ELSE '3418000000000Y' END AS BNMCODE,
                   AMTIND,
                   SUM(BAL_AFT_EIR) AS AMOUNT
            FROM loan
            WHERE PRODCD IN ('34180','34240')
            GROUP BY PRODCD, AMTIND
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, rm_od)

    # Utilised + unutilized gross total approved limit
    total_appr = pl.from_arrow(
        con.execute(
            """
            WITH lna AS (
              SELECT APPRLIM2 AS APPRLIM2, 'I' AS AMTIND, PRODUCT, ACCTYPE, PRODCD, PAIDIND
              FROM loan_raw
              UNION ALL
              SELECT APPRLIM2 AS APPRLIM2, 'I' AS AMTIND, PRODUCT, ACCTYPE, PRODCD, PAIDIND
              FROM uloan_raw
            )
            SELECT '8150000000000Y' AS BNMCODE, AMTIND, SUM(APPRLIM2) AS AMOUNT
            FROM lna
            WHERE NOT (PRODUCT IN (150,151) AND ACCTYPE='OD')
              AND (SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120' OR PRODUCT=973)
              AND PAIDIND NOT IN ('P','C')
            GROUP BY AMTIND
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, total_appr)

    # Undrawn by purpose (5620...)
    undrawn = pl.from_arrow(
        con.execute(
            """
            WITH almx AS (
              SELECT AMTIND, PRODCD, ORIGMT, FISSPURP, SUM(UNDRAWN) AS AMOUNT
              FROM loan_raw
              WHERE PAIDIND NOT IN ('P','C')
                AND (SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120')
              GROUP BY AMTIND, PRODCD, ORIGMT, FISSPURP
              UNION ALL
              SELECT AMTIND, PRODCD, ORIGMT, FISSPURP, SUM(UNDRAWN) AS AMOUNT
              FROM uloan_raw
              WHERE ((COSTCTR BETWEEN 3000 AND 3999) OR COSTCTR IN (4043,4048))
                AND (SUBSTR(PRODCD,1,2)='34' OR PRODCD='54120')
              GROUP BY AMTIND, PRODCD, ORIGMT, FISSPURP
            )
            SELECT '562000000' || FISSPURP || 'Y' AS BNMCODE, AMTIND, SUM(AMOUNT) AS AMOUNT
            FROM almx
            GROUP BY AMTIND, FISSPURP
            """
        ).fetch_arrow_table()
    )
    _append_rows(out_parts, undrawn)

    # Consolidate final BNM.LALM table
    all_rows = pl.concat(out_parts, how="vertical") if out_parts else pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})
    lalm = all_rows.group_by(["BNMCODE", "AMTIND"]).agg(pl.col("AMOUNT").sum())
    con.close()
    return lalm.sort(["BNMCODE", "AMTIND"])


def build_lnaging() -> pl.DataFrame:
    con = _duck()
    _load_parquet(con, "loan_raw", LOAN_FILE)
    lnaging = pl.from_arrow(
        con.execute(
            """
            WITH base AS (
              SELECT
                PRODCD,
                REMAINMT,
                AMTIND,
                SUM(BAL_AFT_EIR) AS AMOUNT
              FROM loan_raw
              WHERE PRODCD IN ('34111','34112','34113','34114','34115','34116','34117','34120','34230','34149')
              GROUP BY PRODCD, REMAINMT, AMTIND
            ),
            mapped AS (
              SELECT
                CASE
                  WHEN REMAINMT IN ('51','52','53','54','55','56','57') THEN '3411000500000Y'
                  WHEN REMAINMT IN ('61','62','63','64','71','72','73') THEN '3411000' || REMAINMT || '0000Y'
                END AS BNMCODE,
                CASE WHEN AMTIND='D' AND PRODCD='34230' THEN 'S'
                     WHEN AMTIND='D' THEN 'T'
                     ELSE 'I'
                END AS LNTYPE,
                AMOUNT
              FROM base
            )
            SELECT BNMCODE, LNTYPE, SUM(AMOUNT) AS AMOUNT
            FROM mapped
            WHERE BNMCODE IS NOT NULL
            GROUP BY BNMCODE, LNTYPE
            ORDER BY LNTYPE, BNMCODE
            """
        ).fetch_arrow_table()
    )
    con.close()
    return lnaging


def _write_asa_print(df: pl.DataFrame, title_lines: list[str], output_file: Path, cols: list[str]) -> None:
    with output_file.open("w", encoding="utf-8") as f:
        line_count = 0

        def put(text: str, new_page: bool = False) -> None:
            nonlocal line_count
            if new_page:
                f.write(f"{ASA_NEW_PAGE}{text}\n")
                line_count = 1
            else:
                f.write(f"{ASA_SPACE}{text}\n")
                line_count += 1

        def page_header(force: bool = False) -> None:
            put(title_lines[0], new_page=True if force else False)
            for t in title_lines[1:]:
                put(t)
            put("")
            put(" ".join([c.ljust(18) for c in cols]))
            put("-" * (len(cols) * 19))

        page_header(force=True)

        for row in df.iter_rows(named=True):
            if line_count >= PAGE_LENGTH:
                page_header(force=True)
            out = []
            for c in cols:
                v = row.get(c)
                if isinstance(v, float):
                    out.append(f"{v:,.2f}".rjust(18))
                else:
                    out.append(str(v).ljust(18))
            put(" ".join(out))


def main() -> None:
    lalm = build_lalm()
    lalm.write_parquet(OUTPUT_LALM)

    title = [
        SDESC,
        "REPORT ON DOMESTIC ASSETS AND LIABILITIES PART II - M&I LOAN",
        f"REPORT DATE : {RDATE}",
    ]
    _write_asa_print(lalm.sort(["BNMCODE", "AMTIND"]), title, OUTPUT_REPORT, ["BNMCODE", "AMTIND", "AMOUNT"])

    lnaging = build_lnaging()
    lnaging.write_parquet(OUTPUT_LNAGING)

    aging_title = [
        "REPORT ID: LALMPBBP",
        SDESC,
        "FINANCIAL ACCOUNTING, FINANCE DIVISION",
        f"AGING OF LOANS AS AT {RDATE}",
    ]
    _write_asa_print(lnaging, aging_title, OUTPUT_AGING_REPORT, ["LNTYPE", "BNMCODE", "AMOUNT"])

    print(f"Wrote {OUTPUT_LALM}")
    print(f"Wrote {OUTPUT_REPORT}")
    print(f"Wrote {OUTPUT_LNAGING}")
    print(f"Wrote {OUTPUT_AGING_REPORT}")


if __name__ == "__main__":
    main()
