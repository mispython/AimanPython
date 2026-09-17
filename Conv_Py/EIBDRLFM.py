#!/usr/bin/env python3
"""
Program : EIBDRLFM.py
Original: EIBDRLFX/EIBDRLFM (UAT version, 27.07.06, ESMR 2006-1012)
Purpose : FISS - New Liquidity Framework report for RM & FCY deposits.
          Invoked by EIBDLIQP.py via %INC PGM(EIBDRLFM) equivalent
          (imported and called as run_eibdrlfm()).

Dependency (format libraries):
    %INC PGM(PBBELF,PBBDPFMT);
    - PBBDPFMT: fdprod_format, ddcustcd_format, ifdcuscd_format are used
      below (FD BIC classification, FCYCA CUSTCD, FDWKLY CUSTCODE).
      SACUSTCD./CAPROD./STATECD. formats from PBBDPFMT are NOT invoked
      anywhere in this program's original SAS logic (they are only used
      by DALWPBBD when building BNM.SAVG/BNM.CURN upstream) so they are
      intentionally not imported here.
    - PBBELF: stated as a dependency in the SAS %INC, but NONE of its
      formats (BRCHCD./CACBRCH./REGIOFF./REGNEW./$CTYPE./BRCHRVR.) are
      referenced anywhere in this program's SAS logic. Kept unimported;
      this comment documents the fact instead of a misleading import.

Local formats (defined by PROC FORMAT inside the original SAS program,
NOT part of PBBDPFMT/PBBELF - reproduced locally below):
    REMFMT  : bucket the remaining-maturity-in-months value (distinct
              bucket set from EIIMRM01.py's own local REMFMT).
    GLPROD  : product -> GL code lookup used only to EXCLUDE 'C999'
              (non-reportable) products from the CA extract.
    $FORATE : CURCODE -> spot exchange rate lookup. Built by the CALLING
              job (EIBDLIQP.py) from FORATE.FORATEBKP and passed in here
              as forate_map, since building it requires PROC SORT/PROC
              FORMAT CNTLIN over an input dataset that is a physical
              input of EIBDLIQP, not of this program.

Inputs required by this module (all pre-cached to parquet by the
CALLING program - this module never resolves or caches its own paths):
    fd_cache      : FD.FD          (ACCTTYPE, CURBAL, CUSTCD, MATDATE,
                                     OPENIND, INTPLAN, BRANCH, ACCTNO,
                                     CURCODE, NAME, FDHOLD, AMTIND)
    current_cache : DEPOSIT.CURRENT (raw, used directly for FCYCA - the
                                     SAME physical cache DALWPBBD used
                                     to build BNM_CURN)
    bnm_savg      : BNM.SAVG&REPTMON&NOWK (polars DataFrame from
                                     DALWPBBD.build_savg_curn_dept)
    bnm_curn      : BNM.CURN&REPTMON&NOWK (polars DataFrame from
                                     DALWPBBD.build_savg_curn_dept)

Outputs:
    LCR.FD/SA/CA/FCYCA&REPTDAY : detail extracts -> persisted as parquet
                                 (LCR DD is a catalogued GDG(+1) SAS
                                 dataset library, not a text report;
                                 generation management is not
                                 reproduced, each run simply writes a
                                 fresh file per REPTDAY, per project
                                 convention for structured non-report
                                 outputs, see DALWPBBD.py).
    NLF.NOTE&REPTYEAR&REPTMON&REPTDAY : persisted as parquet (permanent
                                 SAS dataset, not a report).
    FISS / NSRS  : plain fixed-format ';'-delimited text, RECFM=FB (NOT
                   FBA) per JCL DCB -> NO ASA carriage control.
"""

from pathlib import Path
from datetime import date
from typing import Dict, List, Optional

import duckdb
import polars as pl

from PBBDPFMT import fdprod_format, ddcustcd_format, ifdcuscd_format
# PBBELF import intentionally omitted - see module docstring.

# ============================================================================
# LOCAL FORMAT: REMFMT (distinct bucket set from EIIMRM01.py's own REMFMT)
# ============================================================================
def remfmt_format(value: Optional[float]) -> str:
    """PROC FORMAT VALUE REMFMT (local to EIBDRLFM).
    LOW-0.1='01' 0.1-1='02' 1-3='03' 3-6='04' 6-12='05' OTHER='06'."""
    if value is None:
        return '01'
    if value <= 0.1:
        return '01'
    if value <= 1:
        return '02'
    if value <= 3:
        return '03'
    if value <= 6:
        return '04'
    if value <= 12:
        return '05'
    return '06'


# ============================================================================
# LOCAL FORMAT: GLPROD (product -> GL code; only 'C999' matters - exclusion)
# ============================================================================
_GLPROD_GROUPS: Dict[str, List[int]] = {
    '3301': [117], '3302': [110], '3303': [108], '3304': [118],
    '3305': [157, 102], '3306': [101], '3307': [121],
    '3308': [194, 195, 155, 192, 137, 154, 119, 120, 138, 193],
    '3309': [116], '3311': [114],
    '3313': [91, 93, 179, 174, 175, 100, 156, 198, 90, 85, 86, 87, 88, 89, 180, 197],
    '3314': [123, 176, 196], '3315': [112], '3316': [115], '3317': [111],
    '3318': [113, 189, 177, 190, 178], '3319': [122], '3320': [109],
    '3322': [165, 124, 191], '3323': [159, 125], '3324': [150, 181],
    '3325': [151], '3326': [152], '3327': [170], '3328': [153, 135],
    '3330': [182, 160, 166, 183], '3331': [161], '3332': [162],
    '3334': [164, 167, 168, 169], '7101': [106, 158],
    'C001': [50], 'C002': [51], 'C006': [55], 'C007': [56],
    'C008': [65, 57], 'C009': [58], 'CI01': [60, 61],
    'CI06': [64, 66, 67, 68, 69, 70, 71, 73, 74, 75, 76, 77, 78, 81, 82, 83,
             84, 92, 94, 95, 96, 97, 131, 132, 133, 134, 184, 185, 186, 187,
             188, 20, 21, 22, 23, 24, 25, 40, 41, 42, 43, 26, 27, 13, 14, 15,
             16, 5, 6, 7, 8, 9, 10, 11, 12],
    'HDA0': [53, 63, 103, 163],
}
_GLPROD_MAP: Dict[int, str] = {
    code: label for label, codes in _GLPROD_GROUPS.items() for code in codes
}


def glprod_format(product_code: Optional[int]) -> str:
    if product_code is None:
        return 'C999'
    return _GLPROD_MAP.get(product_code, 'C999')


def forate_format(curcode: Optional[str], forate_map: Dict[str, float]) -> Optional[float]:
    """$FORATE. lookup built by the calling job from FORATE.FORATEBKP.
    No OTHER clause in the original CNTLIN-built format -> missing
    (None) if CURCODE not found, matching SAS PUT() returning blanks
    which convert to numeric-missing on use."""
    if curcode is None:
        return None
    return forate_map.get(curcode)


def _sas_round(x: float) -> float:
    """SAS ROUND() with no scale: nearest integer, halves away from zero."""
    if x >= 0:
        return float(int(x + 0.5))
    return float(-int(-x + 0.5))


# ============================================================================
# %DCLVAR / %REMMTH equivalents (locally scoped to this program's FD step)
# ============================================================================
def _remmth(matdt: date, reptdate: date, rpyr: int, rpmth: int, rpday: int,
            rd_days: List[int]) -> float:
    """%REMMTH macro. MD1-MD12 (MDDAYS) adjustments in the original macro
    are dead code (never read elsewhere) and are not reproduced, matching
    the same fidelity note used in EIIMRM01.py's _remmth()."""
    mdyr, mdmth, mdday = matdt.year, matdt.month, matdt.day
    days_in_rpmth = rd_days[rpmth - 1]
    if mdday > days_in_rpmth:
        mdday = days_in_rpmth
    remy = mdyr - rpyr
    remm = mdmth - rpmth
    remd = mdday - rpday
    return remy * 12 + remm + remd / days_in_rpmth


def _row(bnmcode, amount, amtusd, amtsgd, amthkd, amtaud):
    return {"BNMCODE": bnmcode, "AMOUNT": amount, "AMTUSD": amtusd,
            "AMTSGD": amtsgd, "AMTHKD": amthkd, "AMTAUD": amtaud}


# ============================================================================
# ALWDEPT SELECT(CUSTCODE) branching -> list of BNMCODE suffixes emitted
# ============================================================================
def _alwdept_suffixes(custcode: str) -> List[str]:
    if custcode == '01':
        return ['01000000Y']
    if custcode in ('10', '02', '03', '11', '12'):
        return [f'{custcode}000000Y']
    if custcode in ('20', '13', '17', '30', '31', '04', '05', '06', '32', '33',
                     '34', '35', '36', '37', '38', '39', '40', '45'):
        out = ['20000000Y']
        if custcode == '17':
            out.append('17000000Y')
        return out
    if custcode in ('60', '61', '62', '63', '64', '65', '66', '67', '68', '69',
                     '41', '42', '43', '44', '46', '47', '48', '49', '51', '52',
                     '53', '54', '59', '75', '57'):
        out = ['60000000Y']
        if custcode == '75':
            out.append('75000000Y')
        if custcode == '57':
            out.append('57000000Y')
            out.append('75000000Y')
        return out
    if custcode in ('70', '71', '72', '73', '74'):
        return [f'{custcode}000000Y']
    if custcode in ('76', '77', '78'):
        return ['76000000Y']
    if custcode == '79':
        return ['79000000Y']
    if custcode in ('81', '82', '83', '84'):
        return ['81000000Y']
    if custcode in ('85', '86', '90', '91', '92', '95', '96', '98', '99'):
        return ['85000000Y']
    return []   # OTHERWISE; -> no output


def run_eibdrlfm(
    fd_cache: Path,
    current_cache: Path,
    bnm_savg: pl.DataFrame,
    bnm_curn: pl.DataFrame,
    forate_map: Dict[str, float],
    reptdate: date,
    rpyr: int, rpmth: int, rpday: int,
    rd_days: List[int],
    reptday: str, reptmon: str, reptyear: str,
    lcr_output_dir: Path,
    nlf_output_dir: Path,
) -> Dict[str, List[str]]:
    """
    Runs the EIBDRLFM report body. Returns {"FISS": [...], "NSRS": [...]}
    lines to be written/appended by the calling job (EIBDLIQP.py), which
    owns the FISS/NSRS physical output paths.
    """
    lcr_output_dir.mkdir(parents=True, exist_ok=True)
    nlf_output_dir.mkdir(parents=True, exist_ok=True)
    fiss_lines: List[str] = []
    nsrs_lines: List[str] = []

    # ------------------------------------------------------------------
    # DATA FD ... SET FD.FD;
    # ------------------------------------------------------------------
    con = duckdb.connect(database=":memory:")
    fd_raw = con.execute(f"""
        SELECT
            CAST(ACCTTYPE AS INTEGER) AS ACCTTYPE,
            CAST(CURBAL   AS DOUBLE)  AS CURBAL,
            CAST(CUSTCD   AS INTEGER) AS CUSTCD,
            CAST(MATDATE  AS DATE)    AS MATDT,
            CAST(OPENIND  AS VARCHAR) AS OPENIND,
            CAST(INTPLAN  AS INTEGER) AS INTPLAN,
            CAST(BRANCH   AS INTEGER) AS BRANCH,
            CAST(ACCTNO   AS BIGINT)  AS ACCTNO,
            CAST(CURCODE  AS VARCHAR) AS CURCODE,
            CAST(NAME     AS VARCHAR) AS NAME,
            CAST(FDHOLD   AS VARCHAR) AS FDHOLD
        FROM read_parquet('{fd_cache.as_posix()}')
    """).pl()
    con.close()

    fd_lcr_rows, fd_sum_rows, clsfd_rows = [], [], []

    for r in fd_raw.iter_rows(named=True):
        if r["ACCTTYPE"] in (397, 398):
            continue
        curbal = r["CURBAL"]
        if curbal is None or curbal <= 0:
            continue

        cust = '08' if r["CUSTCD"] in (77, 78, 95, 96) else '09'
        matdt = r["MATDT"]

        if r["OPENIND"] == 'D' or (matdt is not None and (matdt - reptdate).days < 8):
            remmth = 0.1
            remd30 = None if matdt is None else (matdt - reptdate).days / 30.0
        else:
            remmth = _remmth(matdt, reptdate, rpyr, rpmth, rpday, rd_days)
            remd30 = (matdt - reptdate).days / 30.0

        amtusd = amtsgd = amthkd = amtaud = 0.0
        bic = fdprod_format(r["INTPLAN"])
        myramount = None

        if bic == '42630':
            if r["CURCODE"] == 'USD':
                amtusd = curbal
            elif r["CURCODE"] == 'SGD':
                amtsgd = curbal
            elif r["CURCODE"] == 'HKD':
                amthkd = curbal
            elif r["CURCODE"] == 'AUD':
                amtaud = curbal
            bnmcode = '96311' + cust + remfmt_format(remmth) + '0000Y'
            if r["CURCODE"] != 'MYR':
                rate = forate_format(r["CURCODE"], forate_map)
                myramount = None if rate is None else curbal * rate
        elif bic == '42133' or r["ACCTTYPE"] in (302, 315, 394, 396):
            bnmcode = '95317' + cust + remfmt_format(remmth) + '0000Y'
        elif bic == '42132':
            bnmcode = '95315' + cust + remfmt_format(remmth) + '0000Y'
        elif bic == '49999':
            bnmcode = '95999' + cust + remfmt_format(remmth) + '0000Y'
        else:
            bnmcode = '95311' + cust + remfmt_format(remmth) + '0000Y'

        fd_sum_rows.append(_row(bnmcode, curbal, amtusd, amtsgd, amthkd, amtaud))
        fd_lcr_rows.append({
            "BNMCODE": bnmcode, "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"],
            "AMOUNT": curbal, "CURCODE": r["CURCODE"], "CUSTCD": r["CUSTCD"],
            "PRODUCT": r["ACCTTYPE"], "REMMTH": remmth, "REM30D": remd30,
            "FDHOLD": r["FDHOLD"], "INTPLAN": r["INTPLAN"],
        })
        if r["OPENIND"] in ('B', 'C', 'P'):
            clsfd_rows.append({
                "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"], "NAME": r["NAME"],
                "AMOUNT": curbal, "CUSTCD": r["CUSTCD"], "PRODUCT": r["ACCTTYPE"],
                "OPENIND": r["OPENIND"], "CURCODE": r["CURCODE"], "MYRAMOUNT": myramount,
            })

    pl.DataFrame(fd_lcr_rows).write_parquet(lcr_output_dir / f"FD{reptday}.parquet")
    del fd_raw
    print(f"  FD rows -> LCR: {len(fd_lcr_rows):,}  Closed FD (CLSFD): {len(clsfd_rows):,}")

    # ------------------------------------------------------------------
    # DATA SA ... SET BNM.SAVG&REPTMON&NOWK;
    # ------------------------------------------------------------------
    sa_lcr_rows, sa_sum_rows = [], []
    for r in bnm_savg.iter_rows(named=True):
        cust = '08' if r["CUSTCD"] in ('77', '78', '95', '96') else '09'
        bnmcode = '95312' + cust + '01' + '0000Y'
        sa_lcr_rows.append({
            "BNMCODE": bnmcode, "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"],
            "AMOUNT": r["CURBAL"], "CURCODE": r["CURCODE"], "CUSTCD": r["CUSTCD"],
            "PRODUCT": r["PRODUCT"], "REMMTH": None, "REM30D": None,
        })
        if r["PRODCD"] != 'N' or r["CURCODE"] == 'XAU':
            pass  # output LCR.SA (already appended above regardless of gate below)
        if r["PRODCD"] != 'N':
            sa_sum_rows.append(_row(bnmcode, r["CURBAL"], 0.0, 0.0, 0.0, 0.0))

    # LCR.SA gate: only rows meeting (PRODCD NE 'N' OR CURCODE='XAU') are kept
    sa_lcr_rows = [
        row for row, r in zip(sa_lcr_rows, bnm_savg.iter_rows(named=True))
        if r["PRODCD"] != 'N' or r["CURCODE"] == 'XAU'
    ]
    pl.DataFrame(sa_lcr_rows).write_parquet(lcr_output_dir / f"SA{reptday}.parquet")
    print(f"  SA rows -> LCR: {len(sa_lcr_rows):,}")

    # ------------------------------------------------------------------
    # DATA CA ... SET BNM.CURN&REPTMON&NOWK;
    # ------------------------------------------------------------------
    ca_lcr_rows, ca_sum_rows = [], []
    for r in bnm_curn.iter_rows(named=True):
        if glprod_format(r["PRODUCT"]) == 'C999':
            continue
        cust = '08' if r["CUSTCD"] in ('77', '78', '95', '96') else '09'
        bnmcode = '95313' + cust + '01' + '0000Y'
        ca_lcr_rows.append({
            "BNMCODE": bnmcode, "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"],
            "AMOUNT": r["CURBAL"], "CURCODE": r["CURCODE"], "CUSTCD": r["CUSTCD"],
            "PRODUCT": r["PRODUCT"], "REMMTH": None, "REM30D": None,
            "INTRATE": r["INTRATE"], "BILLERIND": r["BILLERIND"],
        })
        ca_sum_rows.append(_row(bnmcode, r["CURBAL"], 0.0, 0.0, 0.0, 0.0))
    pl.DataFrame(ca_lcr_rows).write_parquet(lcr_output_dir / f"CA{reptday}.parquet")
    print(f"  CA rows -> LCR: {len(ca_lcr_rows):,}")

    # ------------------------------------------------------------------
    # DATA FCYCA ... SET DEPOSIT.CURRENT;   (raw current cache, same file
    # DALWPBBD used to build BNM_CURN - shared cache, read independently)
    # ------------------------------------------------------------------
    con = duckdb.connect(database=":memory:")
    cur_raw = con.execute(f"""
        SELECT
            CAST(PRODUCT   AS INTEGER) AS PRODUCT,
            CAST(CUSTCODE  AS INTEGER) AS CUSTCODE,
            CAST(OPENIND   AS VARCHAR) AS OPENIND,
            CAST(CURBAL    AS DOUBLE)  AS CURBAL,
            CAST(CURCODE   AS VARCHAR) AS CURCODE,
            CAST(INTRATE   AS DOUBLE)  AS INTRATE,
            CAST(BILLERIND AS VARCHAR) AS BILLERIND,
            CAST(BRANCH    AS INTEGER) AS BRANCH,
            CAST(ACCTNO    AS BIGINT)  AS ACCTNO,
            CAST(NAME      AS VARCHAR) AS NAME
        FROM read_parquet('{current_cache.as_posix()}')
        WHERE (PRODUCT BETWEEN 400 AND 444) OR PRODUCT IN (450,451,452,453,454)
    """).pl()
    con.close()

    fcyca_lcr_rows, fcyca_sum_rows, clsfcyca_rows = [], [], []
    for r in cur_raw.iter_rows(named=True):
        if r["PRODUCT"] == 413:
            continue
        curcd = ddcustcd_format(r["CUSTCODE"])
        cust = '08' if curcd in ('77', '78', '95', '96') else '09'
        bnmcode = '96313' + cust + '01' + '0000Y'
        curbal = r["CURBAL"]
        amtusd = curbal if r["PRODUCT"] in (400, 420, 440, 450) else 0.0
        amtsgd = curbal if r["PRODUCT"] in (403, 423) else 0.0
        amthkd = curbal if r["PRODUCT"] in (406, 426) else 0.0
        amtaud = curbal if r["PRODUCT"] in (402, 422, 442, 452) else 0.0
        myramount = None
        if r["CURCODE"] != 'MYR':
            rate = forate_format(r["CURCODE"], forate_map)
            myramount = None if rate is None else curbal * rate

        fcyca_sum_rows.append(_row(bnmcode, curbal, amtusd, amtsgd, amthkd, amtaud))
        fcyca_lcr_rows.append({
            "BNMCODE": bnmcode, "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"],
            "AMOUNT": curbal, "CURCODE": r["CURCODE"], "CUSTCD": curcd,
            "PRODUCT": r["PRODUCT"], "REMMTH": None, "REM30D": None,
            "INTRATE": r["INTRATE"], "BILLERIND": r["BILLERIND"],
        })
        if r["OPENIND"] in ('B', 'C', 'P') and curbal != 0:
            clsfcyca_rows.append({
                "BRANCH": r["BRANCH"], "ACCTNO": r["ACCTNO"], "NAME": r["NAME"],
                "AMOUNT": curbal, "CUSTCD": curcd, "PRODUCT": r["PRODUCT"],
                "OPENIND": r["OPENIND"], "CURCODE": r["CURCODE"], "MYRAMOUNT": myramount,
            })
    pl.DataFrame(fcyca_lcr_rows).write_parquet(lcr_output_dir / f"FCYCA{reptday}.parquet")
    del cur_raw
    print(f"  FCYCA rows -> LCR: {len(fcyca_lcr_rows):,}  Closed FCYCA: {len(clsfcyca_rows):,}")

    # ------------------------------------------------------------------
    # PROC SUMMARY (per source) NWAY CLASS BNMCODE -> FD SA CA FCYCA
    # DATA NOTE; SET FD SA CA FCYCA; BNMCODE1=SUBSTR(BNMCODE,1,7)||'000000Y';
    # ------------------------------------------------------------------
    def _group_sum(rows: List[dict], key: str) -> List[dict]:
        groups: Dict[str, dict] = {}
        for r in rows:
            g = groups.setdefault(r[key], {"AMOUNT": 0.0, "AMTUSD": 0.0,
                                             "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
            for f in ("AMOUNT", "AMTUSD", "AMTSGD", "AMTHKD", "AMTAUD"):
                v = r.get(f)
                if v is not None:
                    g[f] += v
        return [{key: k, **v} for k, v in groups.items()]

    all_rows = (
        _group_sum(fd_sum_rows, "BNMCODE") + _group_sum(sa_sum_rows, "BNMCODE")
        + _group_sum(ca_sum_rows, "BNMCODE") + _group_sum(fcyca_sum_rows, "BNMCODE")
    )
    for r in all_rows:
        r["BNMCODE1"] = r["BNMCODE"][:7] + "000000Y"

    note1_rows = _group_sum(all_rows, "BNMCODE1")
    note1_rows.sort(key=lambda r: r["BNMCODE1"])
    note_rows = _group_sum(all_rows, "BNMCODE")
    note_rows.sort(key=lambda r: r["BNMCODE"])

    def _fmt_amt(v, divide_by_1000: bool) -> str:
        if v is None:
            v = 0.0
        v = v / 1000.0 if divide_by_1000 else v
        return str(int(abs(_sas_round(v))))

    for i, r in enumerate(note1_rows):
        if i == 0:
            fiss_lines.append(f"RLFM{reptday}{reptmon}{reptyear}")
            nsrs_lines.append(f"RLFM{reptday}{reptmon}{reptyear}")
        fiss_lines.append(";".join([
            r["BNMCODE1"].ljust(14)[:14], _fmt_amt(r["AMOUNT"], True),
            _fmt_amt(r["AMTUSD"], True), _fmt_amt(r["AMTSGD"], True),
            _fmt_amt(r["AMTHKD"], True), _fmt_amt(r["AMTAUD"], True),
        ]))
        nsrs_lines.append(";".join([
            r["BNMCODE1"].ljust(14)[:14], _fmt_amt(r["AMOUNT"], False),
            _fmt_amt(r["AMTUSD"], False), _fmt_amt(r["AMTSGD"], False),
            _fmt_amt(r["AMTHKD"], False), _fmt_amt(r["AMTAUD"], False),
        ]))

    # DATA NLF.NOTE&REPTYEAR&REPTMON&REPTDAY; SET NOTE; -- persist as parquet
    pl.DataFrame(note_rows).write_parquet(
        nlf_output_dir / f"NOTE{reptyear}{reptmon}{reptday}.parquet"
    )

    for i, r in enumerate(note_rows):
        if i == 0:
            fiss_lines.append("**")
            nsrs_lines.append("**")
        fiss_lines.append(";".join([
            r["BNMCODE"].ljust(14)[:14], _fmt_amt(r["AMOUNT"], True),
            _fmt_amt(r["AMTUSD"], True), _fmt_amt(r["AMTSGD"], True),
            _fmt_amt(r["AMTHKD"], True), _fmt_amt(r["AMTAUD"], True),
        ]))
        nsrs_lines.append(";".join([
            r["BNMCODE"].ljust(14)[:14], _fmt_amt(r["AMOUNT"], False),
            _fmt_amt(r["AMTUSD"], False), _fmt_amt(r["AMTSGD"], False),
            _fmt_amt(r["AMTHKD"], False), _fmt_amt(r["AMTAUD"], False),
        ]))

    # ------------------------------------------------------------------
    # DATA FDWKLY; SET FD.FD; ... PROC SUMMARY -> ALW -> ALWDEPT
    # ------------------------------------------------------------------
    alw_input = []
    for r in fd_raw_for_wkly(fd_cache):
        if r["OPENIND"] not in ('D', 'O'):
            continue
        bic = '42133' if r["ACCTTYPE"] in (302, 315, 394, 396) else None
        if bic is None:
            continue  # BIC missing -> excluded by NWAY without MISSING option
        custcode = ifdcuscd_format(r["CUSTCD"])
        alw_input.append((bic, custcode, r["AMTIND"], r["CURBAL"]))

    alw_groups: Dict[tuple, float] = {}
    for bic, custcode, amtind, curbal in alw_input:
        key = (bic, custcode, amtind)
        alw_groups[key] = (alw_groups.get(key, 0.0) + curbal) if curbal is not None else alw_groups.get(key, 0.0)

    alwdept_groups: Dict[str, float] = {}
    for (bic, custcode, _amtind), amount in alw_groups.items():
        for suffix in _alwdept_suffixes(custcode):
            code = bic + suffix
            alwdept_groups[code] = alwdept_groups.get(code, 0.0) + amount

    for bnmcode in sorted(alwdept_groups):
        amount = alwdept_groups[bnmcode]
        fiss_lines.append(f"{bnmcode.ljust(14)[:14]};{_fmt_amt(amount, True)}")
        nsrs_lines.append(f"{bnmcode.ljust(14)[:14]};{_fmt_amt(amount, False)}")

    # ------------------------------------------------------------------
    # SET CLSFD CLSFCYCA; FILE NSRS MOD; (closed-account listing, NSRS only)
    # ------------------------------------------------------------------
    closed = clsfd_rows + clsfcyca_rows
    if closed:
        nsrs_lines.append(" ")
        nsrs_lines.append("BRANCH;ACCOUNT NO.;CUSTOMER NAME;FORBAL;CURBAL MYR;"
                           "CUSTOMER CODE;PRODUCT CODE;STATUS;")
        for r in closed:
            nsrs_lines.append(";".join([
                str(r["BRANCH"]), str(r["ACCTNO"]), str(r["NAME"] or ""),
                str(r["AMOUNT"]), "" if r["MYRAMOUNT"] is None else str(r["MYRAMOUNT"]),
                str(r["CUSTCD"]), str(r["PRODUCT"]), str(r["OPENIND"]), "",
            ]))

    return {"FISS": fiss_lines, "NSRS": nsrs_lines}


def fd_raw_for_wkly(fd_cache: Path):
    """Re-reads FD.FD for the FDWKLY step (kept as a separate lightweight
    read - only the 4 columns FDWKLY needs - rather than re-using the
    fully-typed fd_raw frame from the FD step, matching the original SAS
    DATA FDWKLY; SET FD.FD; independent re-read)."""
    con = duckdb.connect(database=":memory:")
    df = con.execute(f"""
        SELECT
            CAST(ACCTTYPE AS INTEGER) AS ACCTTYPE,
            CAST(CUSTCD   AS INTEGER) AS CUSTCD,
            CAST(OPENIND  AS VARCHAR) AS OPENIND,
            CAST(CURBAL   AS DOUBLE)  AS CURBAL,
            CAST(AMTIND   AS VARCHAR) AS AMTIND
        FROM read_parquet('{fd_cache.as_posix()}')
    """).pl()
    con.close()
    return df.iter_rows(named=True)
