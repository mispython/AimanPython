#!/usr/bin/env python3
"""
Program : EIBMNPL2.py
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

The original EIBMNPL2 does NOT re-read BNM.LOAN&REPTMON&NOWK / OD.OVERDFT
from disk (DATA LOAN2 MERGEs LOAN(subset) with OD, both re-derived from the
same already-opened librefs); Python mirrors this by re-querying the SAME
cached Parquet paths (LOAN_CACHE / OVERDFT_CACHE) exposed by EIBMNPL1,
rather than re-caching or re-converting anything.

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
    OVERDFT_CACHE,
    _loan_cache_for,
    AsaWriter,
    format_brchcd,
    parse_excessdt,
    parse_toddate,
    compute_bldate,
    comma,
    PAGE_SIZE,
    _num_nocomma
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
            CAST(RISKRTE       AS DOUBLE) AS RISKRTE,
            CAST(BALANCE       AS DOUBLE)  AS BALANCE,
            CAST(APPRLIMT      AS DOUBLE)  AS APPRLIMT,
            (DATE '1960-01-01' + CAST(BLDATE AS INTEGER)) AS BLDATE,
            CAST(SECURE        AS VARCHAR) AS SECURE,
            CAST(OLDNOTEDAYARR AS INTEGER) AS OLDNOTEDAYARR
        FROM read_parquet('{_loan_cache_for(entity).as_posix()}')
        WHERE ACCTYPE = 'LN'
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
        FROM read_parquet('{_loan_cache_for(entity).as_posix()}')
        WHERE ACCTYPE = 'OD'
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
LOAN1_HEADER1 = ("  Obs    BRANCH        ACCTNO    NAME                        "
                 "PRODUCT    CUSTCD    SECTORCD    COLLCD")
LOAN1_HEADER2 = ("  Obs    NOTENO    STATECD    RISKCODE     BALANCE      "
                 "APPRLIMT      BLDATE    SECURE    DAYS")


# def render_mnpl2_print_loan1(asa: AsaWriter, rows: list) -> None:
#     """PROC PRINT DATA=LOAN1 LABEL; BY BRCH; PAGEBY BRCH;
#     VAR BRCH ACCTNO NAME PRODUCT CUSTCD SECTORCD COLLCD NOTENO STATECD
#         RISKRTE BALANCE APPRLIMT BLDATE SECURE DAYS;
#     LABEL BRCH='BRANCH' RISKRTE='RISKCODE';"""
#     title_lines = _mnpl2_title_block("(LOANS)")
#     header = (f"{'OBS':>4} {'BRANCH':<7}{'ACCTNO':>12} {'NAME':<20}{'PRODUCT':>8}"
#               f"{'CUSTCD':>7}{'SECTORCD':>9}{'COLLCD':>7}{'NOTENO':>7}{'STATECD':>8}"
#               f"{'RISKCODE':>9}{'BALANCE':>16}{'APPRLIMT':>16}{'BLDATE':>11}"
#               f"{'SECURE':>7}{'DAYS':>6}")

#     current_branch = None
#     obs = 0
#     for r in rows:
#         if r["BRCH"] != current_branch:
#             current_branch = r["BRCH"]
#             obs = 0
#             asa.new_page(title_lines)
#             asa.add(header)
#         obs += 1
#         bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""
#         asa.ensure_space(1, title_lines)
#         asa.add(
#             f"{obs:>4} {r['BRCH']:<7}{r['ACCTNO']:>12} "
#             f"{(r['NAME'] or '')[:20]:<20}{r['PRODUCT']:>8}"
#             f"{(r['CUSTCD'] or ''):>7}{(r['SECTORCD'] or ''):>9}"
#             f"{(r['COLLCD'] or ''):>7}{r['NOTENO']:>7}{(r['STATECD'] or ''):>8}"
#             f"{(r['RISKRTE'] if r['RISKRTE'] is not None else ''):>9}"
#             f"{comma(r['BALANCE'], 16, 2)}{comma(r['APPRLIMT'], 16, 2)}"
#             f"{bldate_s:>11}{(r['SECURE'] or ''):>7}"
#             f"{(r['DAYS'] if r['DAYS'] is not None else ''):>6}"
#         )


def _loan1_page_capacity(is_continued: bool) -> int:
    """Rows per page for LOAN1's two-block PROC PRINT layout.
    Fixed overhead = 2 title lines + blank + BRANCH= line
    + (1 more if continued) + blank + header1 + blank + header2 + blank
    = 10 (first page of a BY group) / 11 (continued page); the rest of
    PAGE_SIZE is split evenly between the two row blocks."""
    overhead = 11 if is_continued else 10
    return max(1, (PAGE_SIZE - overhead) // 2)


# def _loan1_block1_line(obs: int, r: dict) -> str:
#     return (
#         f"{obs:>4}    {r['BRCH']:<7}{r['ACCTNO']:>13}    "
#         f"{(r['NAME'] or '')[:25]:<25} {r['PRODUCT']:>8}"
#         f"{(r['CUSTCD'] or ''):>7}{(r['SECTORCD'] or ''):>9}"
#         f"{(r['COLLCD'] or ''):>7}"
#     )


# def _loan1_block2_line(obs: int, r: dict) -> str:
#     riskrte = r["RISKRTE"]
#     riskrte_s = "" if riskrte is None else str(int(riskrte))
#     bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""
#     return (
#         f"{obs:>4}    {r['NOTENO']:>6}    {(r['STATECD'] or ''):>7}    "
#         f"{riskrte_s:>8} {comma(r['BALANCE'], 12, 2)}    "
#         f"{comma(r['APPRLIMT'], 12, 2)}    {bldate_s:>8}    "
#         f"{(r['SECURE'] or ''):>6}    "
#         f"{'' if r['DAYS'] is None else r['DAYS']:>4}"
#     )


def _place(buf: list, start: int, text: str) -> None:
    """Overwrite buf[start:start+len(text)] in place, extending buf with
    spaces if the target line isn't long enough yet."""
    end = start + len(text)
    if end > len(buf):
        buf.extend([" "] * (end - len(buf)))
    buf[start:end] = list(text)


def _loan1_block1_line(obs: int, r: dict) -> str:
    buf = [" "] * 105
    _place(buf, 5 - 5, str(obs).rjust(5))                       # OBS   end=5
    _place(buf, 10, (r["BRCH"] or "").ljust(9))                 # BRANCH start=10
    _place(buf, 19, str(r["ACCTNO"]).rjust(10))                 # ACCTNO end=29
    _place(buf, 33, (r["NAME"] or "")[:30].ljust(30))           # NAME   start=33
    _place(buf, 66 - 3, str(r["PRODUCT"]).rjust(3))             # PRODUCT end=66
    _place(buf, 76 - 2, (r["CUSTCD"] or "").rjust(2))           # CUSTCD end=76
    _place(buf, 88 - 4, (r["SECTORCD"] or "").rjust(4))         # SECTORCD end=88
    _place(buf, 99 - 5, (r["COLLCD"] or "").rjust(5))           # COLLCD end=99
    return "".join(buf).rstrip()


def _loan1_block2_line(obs: int, r: dict) -> str:
    riskrte = r["RISKRTE"]
    riskrte_s = "" if riskrte is None else str(int(riskrte))
    bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""

    buf = [" "] * 105
    _place(buf, 0, str(obs).rjust(5))                           # OBS   end=5
    _place(buf, 14 - 5, str(r["NOTENO"]).rjust(5))               # NOTENO end=14
    _place(buf, 23 - 1, (r["STATECD"] or "").rjust(1))           # STATECD end=23
    _place(buf, 35 - 1, riskrte_s.rjust(1))                      # RISKCODE end=35
    _place(buf, 51 - 12, _num_nocomma(r["BALANCE"], 12))         # BALANCE end=51
    _place(buf, 64 - 12, _num_nocomma(r["APPRLIMT"], 12))        # APPRLIMT end=64
    _place(buf, 76 - 8, bldate_s.rjust(8))                       # BLDATE end=76
    _place(buf, 83 - 1, (r["SECURE"] or "").rjust(1))            # SECURE end=83
    _place(buf, 91, "" if r["DAYS"] is None else str(r["DAYS"])) # DAYS start=91 (left-anchored)
    return "".join(buf).rstrip()


def render_mnpl2_print_loan1(asa: AsaWriter, rows: list) -> None:
    """PROC PRINT DATA=LOAN1 LABEL; BY BRCH; PAGEBY BRCH; ...
    Two-block column wrap, OBS numbered continuously across the whole
    report (not reset per BY group), page broken every N rows per the
    fixed-overhead budget in _loan1_page_capacity()."""
    header1 = (f"{'Obs':>4}    {'BRANCH':<7}{'ACCTNO':>13}    "
               f"{'NAME':<25} {'PRODUCT':>8}{'CUSTCD':>7}"
               f"{'SECTORCD':>9}{'COLLCD':>7}")
    header2 = (f"{'Obs':>4}    {'NOTENO':>6}    {'STATECD':>7}    "
               f"{'RISKCODE':>8} {'BALANCE':>12}    {'APPRLIMT':>12}    "
               f"{'BLDATE':>8}    {'SECURE':>6}    {'DAYS':>4}")

    title1, title2 = _mnpl2_title_block("(LOANS)")

    obs = 0
    idx = 0
    current_branch = None
    is_continued = False

    # Pre-group rows by branch (rows are already sorted by BRCH upstream)
    branches: list = []
    for r in rows:
        if not branches or branches[-1][0] != r["BRCH"]:
            branches.append((r["BRCH"], []))
        branches[-1][1].append(r)

    for branch, branch_rows in branches:
        idx = 0
        is_continued = False
        while idx < len(branch_rows):
            n = _loan1_page_capacity(is_continued)
            chunk = branch_rows[idx: idx + n]

            asa.new_page([title1, title2])
            asa.add("")
            asa.add(f"BRANCH={branch}")
            if is_continued:
                asa.add("(continued)")
            asa.add("")
            # asa.add(header1)
            asa.add(LOAN1_HEADER1)
            asa.add("")

            chunk_obs_start = obs
            for r in chunk:
                obs += 1
                asa.add(_loan1_block1_line(obs, r))

            asa.add("")
            # asa.add(header2)
            asa.add(LOAN1_HEADER2)
            asa.add("")

            obs = chunk_obs_start
            for r in chunk:
                obs += 1
                asa.add(_loan1_block2_line(obs, r))

            idx += n
            is_continued = True


# def render_mnpl2_print_loan2(asa: AsaWriter, rows: list) -> None:
#     """PROC PRINT DATA=LOAN2 LABEL; BY BRCH; PAGEBY BRCH;
#     VAR BLDATE NAME CUSTCD PRODUCT RISKCODE COLLCD SECTORCD STATECD ACCTNO
#         BALANCE APPRLIMT EXCESDT TODDT DAYS BRCH; LABEL BRCH='BRANCH';"""
#     title_lines = _mnpl2_title_block("(O/D)")
#     header = (f"{'OBS':>4} {'BLDATE':>10} {'NAME':<20}{'CUSTCD':>7}{'PRODUCT':>8}"
#               f"{'RISKCODE':>9}{'COLLCD':>7}{'SECTORCD':>9}{'STATECD':>8}"
#               f"{'ACCTNO':>12}{'BALANCE':>16}{'APPRLIMT':>16}{'EXCESDT':>10}"
#               f"{'TODDT':>10}{'DAYS':>6} {'BRANCH':<7}")

#     current_branch = None
#     obs = 0
#     for r in rows:
#         if r["BRCH"] != current_branch:
#             current_branch = r["BRCH"]
#             obs = 0
#             asa.new_page(title_lines)
#             asa.add(header)
#         obs += 1
#         bldate_s = r["BLDATE"].strftime("%d/%m/%y") if r["BLDATE"] else ""
#         asa.ensure_space(1, title_lines)
#         asa.add(
#             f"{obs:>4} {bldate_s:>10} {(r['NAME'] or '')[:20]:<20}"
#             f"{(r['CUSTCD'] or ''):>7}{r['PRODUCT']:>8}{r['RISKCODE']:>9}"
#             f"{(r['COLLCD'] or ''):>7}{(r['SECTORCD'] or ''):>9}"
#             f"{(r['STATECD'] or ''):>8}{r['ACCTNO']:>12}"
#             f"{comma(r['BALANCE'], 16, 2)}{comma(r['APPRLIMT'], 16, 2)}"
#             f"{r['EXCESDT']:>10}{r['TODDT']:>10}"
#             f"{(r['DAYS'] if r['DAYS'] is not None else ''):>6} {r['BRCH']:<7}"
#         )


LOAN2_COLS = [
    # (key, label, justify, formatter)
    ("OBS",      "Obs",      "R", lambda v: str(v)),
    ("BLDATE",   "BLDATE",   "R", lambda v: v.strftime("%d/%m/%y") if v else ""),
    ("NAME",     "NAME",     "L", lambda v: (v or "")[:15]),
    ("CUSTCD",   "CUSTCD",   "R", lambda v: v or ""),
    ("PRODUCT",  "PRODUCT",  "R", lambda v: str(v)),
    ("RISKCODE", "RISKCODE", "R", lambda v: v or ""),
    ("COLLCD",   "COLLCD",   "R", lambda v: v or ""),
    ("SECTORCD", "SECTORCD", "R", lambda v: v or ""),
    ("STATECD",  "STATECD",  "R", lambda v: v or ""),
    ("ACCTNO",   "ACCTNO",   "R", lambda v: str(v)),
    ("BALANCE",  "BALANCE",  "R", lambda v: f"{v:.2f}" if v is not None else ""),
    ("APPRLIMT", "APPRLIMT", "R", lambda v: _best_trim(v)),
    ("EXCESDT",  "EXCESDT",  "R", lambda v: v or ""),
    ("TODDT",    "TODDT",    "R", lambda v: v or ""),
    ("DAYS",     "DAYS",     "R", lambda v: "" if v is None else str(v)),
    ("BRCH",     "BRANCH",   "L", lambda v: v or ""),
]
COL_GAP = 1  # spaces between columns; adjust if your reference needs 2


def _best_trim(value) -> str:
    """SAS default BEST-format numeric PUT: no forced decimals, trailing
    .00 dropped when the value is a whole number."""
    if value is None:
        return ""
    v = float(value)
    return str(int(v)) if v.is_integer() else f"{v:.2f}"


def _branch_col_widths(branch_rows: list) -> dict:
    """PROC PRINT's default (non-UNIFORM) column sizing: width = max(label
    length, widest formatted value) computed independently for THIS
    BY-group only -- this is why header spacing shifts between branches."""
    widths = {}
    for key, label, _just, fmt in LOAN2_COLS:
        max_data = max((len(fmt(r[key])) for r in branch_rows), default=0)
        widths[key] = max(len(label), max_data)
    return widths


def _loan2_line(values: dict, widths: dict) -> str:
    parts = []
    for key, _label, just, fmt in LOAN2_COLS:
        text = fmt(values[key])
        w = widths[key]
        parts.append(text.rjust(w) if just == "R" else text.ljust(w))
    return (" " * COL_GAP).join(parts).rstrip()


def _loan2_header(widths: dict) -> str:
    parts = []
    for key, label, _just, _fmt in LOAN2_COLS:
        parts.append(label.center(widths[key]))
    return (" " * COL_GAP).join(parts).rstrip()


def _loan2_page_capacity(is_continued: bool) -> int:
    """title(2) + blank + BRANCH= + [continued] + blank + header + blank
    = 7 (first page) / 8 (continued); remainder goes to data rows."""
    overhead = 8 if is_continued else 7
    return max(1, PAGE_SIZE - overhead)


def render_mnpl2_print_loan2(asa: AsaWriter, rows: list) -> None:
    title1, title2 = _mnpl2_title_block("(O/D)")

    branches: list = []
    for r in rows:
        if not branches or branches[-1][0] != r["BRCH"]:
            branches.append((r["BRCH"], []))
        branches[-1][1].append(r)

    obs = 0
    for branch, branch_rows in branches:
        widths = _branch_col_widths(branch_rows)  # computed once per BY-group
        idx = 0
        is_continued = False
        while idx < len(branch_rows):
            n = _loan2_page_capacity(is_continued)
            chunk = branch_rows[idx: idx + n]

            asa.new_page([title1, title2])
            asa.add("")
            asa.add(f"BRANCH={branch}")
            if is_continued:
                asa.add("(continued)")
            asa.add("")
            asa.add(_loan2_header(widths))
            asa.add("")

            for r in chunk:
                obs += 1
                values = dict(r)
                values["OBS"] = obs
                asa.add(_loan2_line(values, widths))

            idx += n
            is_continued = True


# ============================================================================
# ENTRY POINT (called from EIBMNPL1.main() once per entity, in lieu of the
# plain "%INC PGM1(EIBMNPL2)" inline-execution -- see EIBMNPL1.py docstring)
# ============================================================================
def run(entity: str, asa: AsaWriter) -> None:
    loan1 = build_mnpl2_loan1(entity)
    loan2 = build_mnpl2_loan2(entity)
    render_mnpl2_print_loan1(asa, loan1)
    render_mnpl2_print_loan2(asa, loan2)
