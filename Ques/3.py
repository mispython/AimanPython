#!/usr/bin/env python3
"""
Program : EIBMNPL2
Purpose : Continuation program of EIBMNPL1 (original SAS: %INC PGM1(EIBMNPL2)
          run inline, in the same session, after EIBMNPL1's ageing table).
          Produces the "TOTAL OVERDUE LOANS DETAILS" PROC PRINT listings:
            - LOAN1 (loans, PAGEBY BRCH)
            - LOAN2 (O/D, PAGEBY BRCH)
          appended to the SAME ODTLLIST.COLD output opened by EIBMNPL1
          (RECFM=FBA, ASA carriage control, LRECL=136).

Dependency:
    All shared paths, caches, report-date values, and helper functions are
    imported directly from EIBMNPL1.py ("from EIBMNPL1 import ..."), since
    the original %INC PGM1(EIBMNPL2) executes inline in EIBMNPL1's already
    -established SAS session (same librefs BNM/OD, same REPTDATE macro
    variables, same open ODTLLIST destination).
    - PBBELF  : format_brchcd() used again here (PUT(BRANCH,BRCHCD.)).
    - PBBLNFMT: same as EIBMNPL1 -- included at session level but no live
      PUT(x,<PBBLNFMT format>.) call appears in EIBMNPL2's body either.
      # from PBBLNFMT import ...   (NOT USED -- no live format call)

CACHING NOTE (convert -> use -> delete):
    LOAN_CACHE and OVERDFT_CACHE are imported directly from EIBMNPL1. They
    are only valid Paths WHILE EIBMNPL1.main() has them set (i.e. between
    _convert_inputs() and _cleanup_inputs()) -- this module is only ever
    imported and its run() function only ever called from inside that
    window (see EIBMNPL1.main()'s deferred "import EIBMNPL2" placement),
    so no re-conversion and no reference to a deleted file ever occurs.

The original EIBMNPL2 does NOT re-read BNM.LOAN&REPTMON&NOWK / OD.OVERDFT
from disk (DATA LOAN2 MERGEs LOAN(subset) with OD, both re-derived from the
same already-opened librefs); Python mirrors this by re-querying the SAME
temporary Parquet files (LOAN_CACHE / OVERDFT_CACHE) that EIBMNPL1 already
converted for this run, rather than converting anything a second time.

EIBMNPL2's first %PRT call (PROC PRINTTO PRINT=PRINT) targets the default
SAS listing (SASLIST DD is commented out in the JCL), so it is not captured
to any catalogued dataset in the original job -- not written to a file here
either, matching that behaviour. Only the second %PRT call
(PROC PRINTTO PRINT=ODTLLIST, no NEW -> append) is captured, i.e. the
render_mnpl2_print_loan1/2() calls below.
"""

from EIBMNPL1 import (
    REPTDATE,
    RDATE,
    LOAN_CACHE,
    OVERDFT_CACHE,
    AsaWriter,
    format_brchcd,
    parse_excessdt,
    parse_toddate,
    compute_bldate,
    comma,
)
from PBBELF import format_brchcd  # noqa: F811  (re-import kept explicit for
                                   # parity with EIBMNPL2's own %INC PBBELF;
                                   # identical function already imported above)

import duckdb


# ============================================================================
# LOAN1 (loans detail)
# ============================================================================
def build_mnpl2_loan1(entity: str) -> list:
    """DATA LOAN1: KEEP BRCH ACCTNO NAME PRODUCT CUSTCD SECTORCD COLLCD
    NOTENO STATECD RISKRTE BALANCE APPRLIMT BLDATE SECURE DAYS;
    (no RENAME BRCH=BRANCH here -- output var name stays BRCH)."""
    print(f"\nStep 8 [{entity}]: EIBMNPL2 LOAN1 (loans detail)...")
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT
            CAST(BRANCH        AS INTEGER) AS BRANCH,
            CAST(ACCTNO        AS BIGINT)  AS ACCTNO,
            CAST(NAME          AS VARCHAR) AS NAME,
            CAST(PRODUCT       AS INTEGER) AS PRODUCT,
            CAST(CUSTCD        AS VARCHAR) AS CUSTCD,
            CAST(SECTORCD      AS VARCHAR) AS SECTORCD,
            CAST(COLLCD        AS VARCHAR) AS COLLCD,
            CAST(NOTENO        AS INTEGER) AS NOTENO,
            CAST(STATECD       AS VARCHAR) AS STATECD,
            CAST(RISKRTE       AS INTEGER) AS RISKRTE,
            CAST(BALANCE       AS DOUBLE)  AS BALANCE,
            CAST(APPRLIMT      AS DOUBLE)  AS APPRLIMT,
            CAST(BLDATE        AS DATE)    AS BLDATE,
            CAST(SECURE        AS VARCHAR) AS SECURE,
            CAST(OLDNOTEDAYARR AS INTEGER) AS OLDNOTEDAYARR
        FROM read_parquet('{LOAN_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}'
          AND ACCTYPE = 'LN'
          AND BRANCH IS NOT NULL
          AND BALANCE >= 1.00
          AND PRODUCT NOT IN (517, 500)
    """).pl()
    con.close()

    out = []
    for r in raw.iter_rows(named=True):
        bldate = r["BLDATE"]
        days = (REPTDATE - bldate).days if bldate is not None else None

        oldarr = r["OLDNOTEDAYARR"]
        noteno = r["NOTENO"]
        if oldarr is not None and oldarr > 0 and 98000 <= noteno <= 98999:
            if days is None or days < 0:
                days = 0
            days = days + oldarr   # SUM(DAYS,OLDNOTEDAYARR) -- ignores missing

        riskrte = r["RISKRTE"]
        if riskrte not in (1, 2, 3, 4):
            if days is None or days < 30:
                continue   # DELETE

        out.append({
            "BRCH": format_brchcd(r["BRANCH"]), "ACCTNO": r["ACCTNO"],
            "NAME": r["NAME"], "PRODUCT": r["PRODUCT"], "CUSTCD": r["CUSTCD"],
            "SECTORCD": r["SECTORCD"], "COLLCD": r["COLLCD"],
            "NOTENO": noteno, "STATECD": r["STATECD"], "RISKRTE": riskrte,
            "BALANCE": r["BALANCE"], "APPRLIMT": r["APPRLIMT"],
            "BLDATE": bldate, "SECURE": r["SECURE"], "DAYS": days,
        })

    out.sort(key=lambda x: (x["BRCH"], x["DAYS"] if x["DAYS"] is not None else -1, x["RISKRTE"] or 0))
    print(f"  LOAN1 (mnpl2) rows: {len(out):,}")
    return out


# ============================================================================
# LOAN2 (O/D detail)
# ============================================================================
def build_mnpl2_loan2(entity: str) -> list:
    """DATA LOAN2: MERGE LOAN(ACCTYPE='OD') OD; BY ACCTNO;"""
    print(f"\nStep 9 [{entity}]: EIBMNPL2 LOAN2 (O/D detail)...")
    con = duckdb.connect(database=":memory:")
    od_base = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(NAME     AS VARCHAR) AS NAME,
            CAST(PRODUCT  AS INTEGER) AS PRODUCT,
            CAST(CUSTCD   AS VARCHAR) AS CUSTCD,
            CAST(SECTORCD AS VARCHAR) AS SECTORCD,
            CAST(COLLCD   AS VARCHAR) AS COLLCD,
            CAST(STATECD  AS VARCHAR) AS STATECD,
            CAST(BALANCE  AS DOUBLE)  AS BALANCE,
            CAST(APPRLIMT AS DOUBLE)  AS APPRLIMT
        FROM read_parquet('{LOAN_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}' AND ACCTYPE = 'OD'
        ORDER BY ACCTNO
    """).pl()
    od_ref = con.execute(f"""
        SELECT
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(EXCESSDT AS BIGINT)  AS EXCESSDT,
            CAST(TODDATE  AS BIGINT)  AS TODDATE,
            CAST(RISKCODE AS VARCHAR) AS RISKCODE
        FROM read_parquet('{OVERDFT_CACHE.as_posix()}')
        WHERE ENTITY_CD = '{entity}'
          AND (EXCESSDT > 0 OR TODDATE > 0)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY ACCTNO) = 1
    """).pl()
    con.close()

    od_ref_map = {r["ACCTNO"]: r for r in od_ref.iter_rows(named=True)}

    out = []
    for r in od_base.iter_rows(named=True):
        od = od_ref_map.get(r["ACCTNO"])
        if od is None:
            continue
        if r["PRODUCT"] in (517, 500):
            continue

        excessdt, toddate = od["EXCESSDT"], od["TODDATE"]
        excdate = parse_excessdt(excessdt) if excessdt else None
        toddt1 = parse_toddate(toddate) if toddate else None
        excesdt_str = excdate.strftime("%d/%m/%y") if excdate else ""
        toddt_str = toddt1.strftime("%d/%m/%y") if toddt1 else ""

        bldate = compute_bldate(excessdt, toddate)
        days = (REPTDATE - bldate).days + 1 if bldate is not None else None

        riskcode = od["RISKCODE"]
        if riskcode not in ("1", "2", "3", "4"):
            if days is None or days < 30:
                continue   # DELETE

        out.append({
            "BRCH": format_brchcd(r["BRANCH"]), "ACCTNO": r["ACCTNO"],
            "NAME": r["NAME"], "PRODUCT": r["PRODUCT"], "CUSTCD": r["CUSTCD"],
            "SECTORCD": r["SECTORCD"], "COLLCD": r["COLLCD"],
            "STATECD": r["STATECD"], "RISKCODE": riskcode,
            "BALANCE": r["BALANCE"], "APPRLIMT": r["APPRLIMT"],
            "BLDATE": bldate, "DAYS": days,
            "EXCESDT": excesdt_str, "TODDT": toddt_str,
        })

    out.sort(key=lambda x: (x["BRCH"], x["DAYS"] if x["DAYS"] is not None else -1, x["RISKCODE"] or ""))
    print(f"  LOAN2 (mnpl2) rows: {len(out):,}")
    return out


def _mnpl2_title_block(suffix: str) -> list:
    return ["TOTAL OVERDUE LOANS DETAILS", f"AS AT {RDATE} {suffix}"]


# ============================================================================
# PROC PRINT renderers (appended to EIBMNPL1's ASA writer / ODTLLIST.COLD)
# ============================================================================
def render_mnpl2_print_loan1(asa: AsaWriter, rows: list) -> None:
    """PROC PRINT DATA=LOAN1 LABEL; BY BRCH; PAGEBY BRCH;
    VAR BRCH ACCTNO NAME PRODUCT CUSTCD SECTORCD COLLCD NOTENO STATECD
        RISKRTE BALANCE APPRLIMT BLDATE SECURE DAYS;
    LABEL BRCH='BRANCH' RISKRTE='RISKCODE';"""
    title_lines = _mnpl2_title_block("(LOANS)")
    header = (f"{'OBS':>4} {'BRANCH':<7}{'ACCTNO':>12} {'NAME':<20}{'PRODUCT':>8}"
              f"{'CUSTCD':>7}{'SECTORCD':>9}{'COLLCD':>7}{'NOTENO':>7}{'STATECD':>8}"
              f"{'RISKCODE':>9}{'BALANCE':>16}{'APPRLIMT':>16}{'BLDATE':>11}"
              f"{'SECURE':>7}{'DAYS':>6}")

    current_branch = None
    obs = 0
    for r in rows:
        if r["BRCH"] != current_branch:
            current_branch = r["BRCH"]
            obs = 0
            asa.new_page(title_lines)
            asa.add(header)
        obs += 1
        bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""
        asa.ensure_space(1, title_lines)
        asa.add(
            f"{obs:>4} {r['BRCH']:<7}{r['ACCTNO']:>12} "
            f"{(r['NAME'] or '')[:20]:<20}{r['PRODUCT']:>8}"
            f"{(r['CUSTCD'] or ''):>7}{(r['SECTORCD'] or ''):>9}"
            f"{(r['COLLCD'] or ''):>7}{r['NOTENO']:>7}{(r['STATECD'] or ''):>8}"
            f"{(r['RISKRTE'] if r['RISKRTE'] is not None else ''):>9}"
            f"{comma(r['BALANCE'], 16, 2)}{comma(r['APPRLIMT'], 16, 2)}"
            f"{bldate_s:>11}{(r['SECURE'] or ''):>7}"
            f"{(r['DAYS'] if r['DAYS'] is not None else ''):>6}"
        )


def render_mnpl2_print_loan2(asa: AsaWriter, rows: list) -> None:
    """PROC PRINT DATA=LOAN2 LABEL; BY BRCH; PAGEBY BRCH;
    VAR BLDATE NAME CUSTCD PRODUCT RISKCODE COLLCD SECTORCD STATECD ACCTNO
        BALANCE APPRLIMT EXCESDT TODDT DAYS BRCH; LABEL BRCH='BRANCH';"""
    title_lines = _mnpl2_title_block("(O/D)")
    header = (f"{'OBS':>4} {'BLDATE':>10} {'NAME':<20}{'CUSTCD':>7}{'PRODUCT':>8}"
              f"{'RISKCODE':>9}{'COLLCD':>7}{'SECTORCD':>9}{'STATECD':>8}"
              f"{'ACCTNO':>12}{'BALANCE':>16}{'APPRLIMT':>16}{'EXCESDT':>10}"
              f"{'TODDT':>10}{'DAYS':>6} {'BRANCH':<7}")

    current_branch = None
    obs = 0
    for r in rows:
        if r["BRCH"] != current_branch:
            current_branch = r["BRCH"]
            obs = 0
            asa.new_page(title_lines)
            asa.add(header)
        obs += 1
        bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""
        asa.ensure_space(1, title_lines)
        asa.add(
            f"{obs:>4} {bldate_s:>10} {(r['NAME'] or '')[:20]:<20}"
            f"{(r['CUSTCD'] or ''):>7}{r['PRODUCT']:>8}{r['RISKCODE']:>9}"
            f"{(r['COLLCD'] or ''):>7}{(r['SECTORCD'] or ''):>9}"
            f"{(r['STATECD'] or ''):>8}{r['ACCTNO']:>12}"
            f"{comma(r['BALANCE'], 16, 2)}{comma(r['APPRLIMT'], 16, 2)}"
            f"{r['EXCESDT']:>10}{r['TODDT']:>10}"
            f"{(r['DAYS'] if r['DAYS'] is not None else ''):>6} {r['BRCH']:<7}"
        )


# ============================================================================
# ENTRY POINT (called from EIBMNPL1.main() once per entity, in lieu of the
# plain "%INC PGM1(EIBMNPL2)" inline-execution -- see EIBMNPL1.py docstring)
# ============================================================================
def run(entity: str, asa: AsaWriter) -> None:
    loan1 = build_mnpl2_loan1(entity)
    loan2 = build_mnpl2_loan2(entity)
    render_mnpl2_print_loan1(asa, loan1)
    render_mnpl2_print_loan2(asa, loan2)
