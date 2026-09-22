#!/usr/bin/env python3
"""
Program : KALMLIQ4.py
Purpose : NLF Kapiti bond-sale item allocation fragment (originally %INC
          PGM(KALMLIQ4) inside KALMLIQ). Filters BNMK.K3TBL&REPTMON&NOWK
          for UTREF='RRS' AND UTSTY='MGS' AND UTDLP='MSS' and derives an
          '820'/'830' item BNM code by customer type.

          NOTE: the resulting K3TBL3 is never referenced anywhere else in
          KALMLIQ or its callers -- it is preserved here as an orphaned
          dataset for source fidelity (see project convention on
          preserving dead SAS artefacts). Its result is built but never
          merged downstream.

          Designed to be imported by KALMLIQ.py, mirroring %INC
          semantics. Owns no physical path of its own.
"""
from pathlib import Path
from datetime import date

import duckdb
import polars as pl

from PBBELF import format_ctype

IREP = {"01", "02", "11", "12", "81"}
NREP = {"13", "17", "20", "60", "71", "72", "74", "76", "79", "85"}

_SCHEMA = {"PART": pl.Utf8, "ITEM": pl.Utf8, "AMOUNT": pl.Float64, "MATDT": pl.Date, "CUST": pl.Utf8}


def _parse_sas_date(raw) -> date | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    y, m, d = s.split("-")[:3]
    return date(int(y), int(m), int(d[:2]))


def build_k3tbl3(k3tbl_cache: Path, reptdate: date) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(UTPCP  AS DOUBLE)  AS UTPCP,
            CAST(UTFCV  AS DOUBLE)  AS UTFCV,
            CAST(UTAICT AS DOUBLE)  AS UTAICT,
            CAST(UTCTP  AS VARCHAR) AS UTCTP,
            CAST(UTIDT  AS VARCHAR) AS UTIDT,
            CAST(ISSDT  AS VARCHAR) AS ISSDT
        FROM read_parquet('{k3tbl_cache.as_posix()}')
        WHERE UTREF = 'RRS' AND UTSTY = 'MGS' AND UTDLP = 'MSS'
    """).pl()
    con.close()

    rows = []
    for r in raw.iter_rows(named=True):
        issdt = _parse_sas_date(r["ISSDT"])
        if issdt is not None and issdt > reptdate:  # IF ISSDT > REPTDATE THEN DELETE
            continue
        amount = (r["UTPCP"] or 0.0) * (r["UTFCV"] or 0.0) * 0.01
        amount = amount + (r["UTAICT"] or 0.0)  # SALES PROCEEDS
        cust = format_ctype(r["UTCTP"] or "")
        matdt = _parse_sas_date(r["UTIDT"]) if r["UTIDT"] not in (None, " ", "") else None
        if cust in NREP:
            item = "830"
        elif cust in IREP:
            item = "820"
        else:
            item = None
        if cust.strip() == "":  # IF CUST NE '  '
            continue
        rows.append({"PART": "95", "ITEM": item, "AMOUNT": amount, "MATDT": matdt, "CUST": cust})

    return pl.DataFrame(rows, schema=_SCHEMA) if rows else pl.DataFrame(schema=_SCHEMA)
