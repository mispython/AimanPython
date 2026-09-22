#!/usr/bin/env python3
"""
Program : EIIMRLFM.py
Purpose : New Liquidity Framework (FISS submission) -- Islamic book
          (PIBB) variant of EIBMRLFM. Originally %INC PGM(EIIMRLFM)
          inside EIIMLIQP.

          Differences from EIBMRLFM (the PBB variant), per the SAS
          source:
            - %LET FCY=(...) excludes 815,816,817 (present in EIBMRLFM's set)
            - PROC FORMAT VALUE GLPROD carries a larger 'CI06' set and adds
              017/018/019 -> '3313' and 061 -> 'CI01'
            - FD.FD uses ACCTTYPE ^= 398 (not 397) and a different BIC ->
              BNMCODE mapping (adds '95317'/'95999' buckets)
            - FCYCA reads PRODUCT IN (400:444) OR (450:454) with different
              per-currency bucket assignments (400/440/450 -> USD, etc.)
            - PAY is sourced from PAY.ILNPAY&REPTMON&NOWK&REPTYEA2 (not
              PAY.LNPAY)
            - No DUAL CURRENCY INVESTMENT (DCI/DCIW) section exists in
              this program -- LCR.DCI is never built or referenced
            - No LCR.VOSTRO section either
          Everything else (NOTE loan-schedule build, SA, undrawn portion,
          NID, KAPITI items via KALMLIQ/KALMLIFE, distribution profile,
          TOP-100 reports) is identical in shape to EIBMRLFM, so those
          helpers are imported and reused rather than duplicated.

          Dependencies: PBBLNFMT.format_liqpfmt, PBBDPFMT.fdprod_format /
          ddcustcd_format (via the reused EIBMRLFM helpers), KALMLIQ.build_kalmliq,
          KALMLIFE.build_k3fei. PBBELF is %INC'd in the SAS source but no
          PUT(var,fmt.) call from it appears in this program's body --
          kept as comment only.

          Designed to be imported by EIIMLIQP.py, mirroring %INC
          semantics. Owns no physical path of its own -- every cache path
          and REPTDATE context are supplied by the calling job.
"""
from pathlib import Path

import duckdb
import polars as pl

from PBBDPFMT import fdprod_format
from KALMLIQ import build_kalmliq
from KALMLIFE import build_k3fei

# Reused as-is from EIBMRLFM -- identical logic in both SAS programs.
from EIBMRLFM import (
    _BNMCODE_SCHEMA,
    _remfmt,
    _remmth,
    _summarize,
    _build_note,
    _build_sa,
    _build_undrawn,
    _build_nid,
    _write_fiss_nsrs,
    _write_suppl_report,
    _build_top100,
    _write_top100_report,
)

FCY_PRODUCTS = {800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814,
                851, 852, 853, 854, 855, 856, 857, 858, 859, 860}

GLPROD_MAP = {
    117: "3301", 110: "3302", 108: "3303", 118: "3304", 157: "3305", 102: "3305", 101: "3306",
    121: "3307", 194: "3308", 195: "3308", 155: "3308", 192: "3308", 137: "3308", 154: "3308",
    119: "3308", 120: "3308", 138: "3308", 193: "3308", 116: "3309", 114: "3311", 85: "3311",
    86: "3311", 87: "3313", 88: "3313", 89: "3313", 91: "3313", 179: "3313", 174: "3313",
    175: "3313", 100: "3313", 156: "3313", 198: "3313", 90: "3313", 93: "3313", 180: "3313",
    197: "3313", 17: "3313", 18: "3313", 19: "3313",
    123: "3314", 176: "3314", 196: "3314", 112: "3315", 115: "3316", 111: "3317",
    113: "3318", 135: "3318", 189: "3318", 177: "3318", 190: "3318", 178: "3318", 122: "3319",
    109: "3320", 165: "3322", 124: "3322", 191: "3322", 159: "3323", 125: "3323", 150: "3324",
    181: "3324", 151: "3325", 152: "3326", 170: "3327", 153: "3328", 182: "3330", 183: "3330",
    160: "3330", 166: "3330", 167: "3330", 168: "3330", 169: "3330", 161: "3331", 162: "3332",
    164: "3334", 106: "7101", 158: "7101", 50: "C001", 51: "C002", 55: "C006", 56: "C007",
    65: "C008", 57: "C008", 58: "C009", 60: "CI01", 61: "CI01",
    64: "CI06", 66: "CI06", 67: "CI06", 68: "CI06", 69: "CI06", 70: "CI06", 71: "CI06",
    73: "CI06", 74: "CI06", 81: "CI06", 82: "CI06", 83: "CI06", 84: "CI06", 92: "CI06",
    94: "CI06", 95: "CI06", 96: "CI06", 97: "CI06", 133: "CI06", 134: "CI06", 184: "CI06",
    185: "CI06", 186: "CI06", 187: "CI06", 188: "CI06",
    20: "CI06", 21: "CI06", 22: "CI06", 23: "CI06", 24: "CI06", 25: "CI06",
    75: "CI06", 76: "CI06", 46: "CI06", 47: "CI06", 48: "CI06", 49: "CI06", 45: "CI06",
    13: "CI06", 14: "CI06", 15: "CI06", 16: "CI06", 5: "CI06", 6: "CI06", 7: "CI06", 8: "CI06",
    53: "HDA0", 63: "HDA0", 103: "HDA0", 163: "HDA0",
}


def _glprod(product) -> str:
    return GLPROD_MAP.get(product, "C999")


def _build_fd(fd_fd_cache, ctx) -> pl.DataFrame:
    """DATA FD; SET FD.FD; IF ACCTTYPE ^= 398; IF CURBAL > 0; ...
    BIC-driven mapping differs from EIBMRLFM (adds 95317/95999 buckets,
    folds ACCTTYPE IN (302,315,394,396,313,314) into the 42133 check)."""
    con = duckdb.connect(database=":memory:")
    fd = con.execute(f"""
        SELECT * FROM read_parquet('{fd_fd_cache.as_posix()}') WHERE ACCTTYPE <> 398 AND CURBAL > 0
    """).pl()
    con.close()
    fd317_types = {302, 315, 394, 396, 313, 314}
    rows = []
    for r in fd.iter_rows(named=True):
        cust = "08" if r.get("CUSTCD") in (77, 78, 95, 96) else "09"
        matdt, openind = r.get("MATDATE"), r.get("OPENIND")
        if openind == "D" or (matdt is not None and (matdt - ctx["reptdate"]).days < 8):
            remmth = 0.1
        else:
            remmth, _ = _remmth(ctx, matdt)
        bic = fdprod_format(r.get("INTPLAN"))
        curbal, curcode, accttype = r.get("CURBAL") or 0.0, r.get("CURCODE"), r.get("ACCTTYPE")
        amtusd = amtsgd = amthkd = amtaud = 0.0
        bnmcode95311 = f"95311{cust}{_remfmt(remmth)}0000Y"
        if bic == "42630":
            if curcode == "USD":
                amtusd = curbal
            elif curcode == "SGD":
                amtsgd = curbal
            elif curcode == "HKD":
                amthkd = curbal
            elif curcode == "AUD":
                amtaud = curbal
            rows.append({"BNMCODE": f"96311{cust}{_remfmt(remmth)}0000Y", "AMOUNT": curbal,
                         "AMTUSD": amtusd, "AMTSGD": amtsgd, "AMTHKD": amthkd, "AMTAUD": amtaud})
        if bic == "42133" or accttype in fd317_types:
            bnmcode = f"95317{cust}{_remfmt(remmth)}0000Y"
        elif bic == "42132":
            bnmcode = f"95315{cust}{_remfmt(remmth)}0000Y"
        elif bic == "49999":
            bnmcode = f"95999{cust}{_remfmt(remmth)}0000Y"
        else:
            bnmcode = bnmcode95311
        rows.append({"BNMCODE": bnmcode, "AMOUNT": curbal, "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_ca(bnm_curn_cache) -> pl.DataFrame:
    con = duckdb.connect(database=":memory:")
    ca_raw = con.execute(f"SELECT * FROM read_parquet('{bnm_curn_cache.as_posix()}')").pl()
    con.close()
    rows = []
    for r in ca_raw.iter_rows(named=True):
        if _glprod(r.get("PRODUCT")) == "C999":
            continue
        if str(r.get("PRODCD") or "")[:3] not in ("421", "423"):
            continue
        cust = "08" if r.get("CUSTCD") in ("77", "78", "95", "96") else "09"
        rows.append({"BNMCODE": f"95313{cust}010000Y", "AMOUNT": r.get("CURBAL"),
                     "AMTUSD": 0.0, "AMTSGD": 0.0, "AMTHKD": 0.0, "AMTAUD": 0.0})
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def _build_fcyca(deposit_current_cache) -> pl.DataFrame:
    """IF (400<=PRODUCT<=444) OR PRODUCT IN (450:454); IF PRODUCT=413 THEN
    DELETE; -- distinct per-currency bucket assignments from EIBMRLFM."""
    con = duckdb.connect(database=":memory:")
    raw = con.execute(f"""
        SELECT * FROM read_parquet('{deposit_current_cache.as_posix()}')
        WHERE (PRODUCT BETWEEN 400 AND 444 OR PRODUCT BETWEEN 450 AND 454) AND PRODUCT <> 413
    """).pl()
    con.close()
    rows = []
    for r in raw.iter_rows(named=True):
        custcd = str(r.get("CUSTCD") or "")
        cust = "08" if custcd in ("77", "78", "95", "96") else "09"
        product, curbal = r.get("PRODUCT"), r.get("CURBAL") or 0.0
        rows.append({
            "BNMCODE": f"96313{cust}010000Y", "AMOUNT": curbal,
            "AMTUSD": curbal if product in (400, 440, 450) else 0.0,
            "AMTSGD": curbal if product == 403 else 0.0,
            "AMTHKD": curbal if product == 406 else 0.0,
            "AMTAUD": curbal if product in (402, 442, 452) else 0.0,
        })
    return pl.DataFrame(rows, schema=_BNMCODE_SCHEMA) if rows else pl.DataFrame(schema=_BNMCODE_SCHEMA)


def run_eiimrlfm(
    bnm1_loan_cache: Path, bnm1_uloan_cache: Path, lncomm_cache: Path, provsub_txt_path: Path, ilnpay_cache: Path,
    fd_fd_cache: Path, bnm_savg_cache: Path, bnm_curn_cache: Path, deposit_current_cache: Path,
    nid_rnid_cache: Path, k1tbl_cache: Path, k3tbl_cache: Path,
    cisln_deposit_cache: Path, cisdp_deposit_cache: Path, deposit_fd_cache: Path,
    ctx: dict, output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    inst = "PBB"  # %LET INST='PBB'; carried over unchanged from KALMLIQ's own call in EIIMRLFM

    print("  Building NOTE (loans)...")
    note = _build_note(bnm1_loan_cache, lncomm_cache, provsub_txt_path, ilnpay_cache, ctx)
    print("  Building FD / SA / CA / FCYCA...")
    fd = _build_fd(fd_fd_cache, ctx)
    sa = _build_sa(bnm_savg_cache)
    ca = _build_ca(bnm_curn_cache)
    fcyca = _build_fcyca(deposit_current_cache)
    print("  Building UNOTE (undrawn portion)...")
    unote = _build_undrawn(bnm1_loan_cache, bnm1_uloan_cache, lncomm_cache, ctx)
    print("  Building NID...")
    nid = _build_nid(nid_rnid_cache, ctx)

    print("  Building KAPITI items (KALMLIQ + KALMLIFE)...")
    ktbl, dist_summary = build_kalmliq(k1tbl_cache, k3tbl_cache, ctx["reptdate"], ctx["rpyr"], ctx["rpmth"], ctx["rpday"], ctx["rd_days"], inst=inst)
    ktbl = ktbl.with_columns([pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD")])
    k3fei = build_k3fei(k3tbl_cache, ctx["reptmon"], ctx["reptyear"])
    k3fei_norm = k3fei.rename({"ITCODE": "BNMCODE"}).with_columns([
        pl.lit(0.0).alias("AMTUSD"), pl.lit(0.0).alias("AMTSGD"),
        pl.lit(0.0).alias("AMTHKD"), pl.lit(0.0).alias("AMTAUD"),
    ]).select(["BNMCODE", "AMOUNT", "AMTUSD", "AMTSGD", "AMTHKD", "AMTAUD"])

    print("  Consolidating and summarising...")
    # SET NOTE FD SA CA UNOTE FCYCA NID; -- no DCI/DCIW in this program.
    combined = pl.concat([
        _summarize(note), _summarize(fd), _summarize(sa), _summarize(ca),
        _summarize(unote), _summarize(fcyca), _summarize(nid),
        _summarize(ktbl), k3fei_norm,
    ], how="diagonal_relaxed")
    note_final = combined.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum(), pl.col("AMTUSD").sum(), pl.col("AMTSGD").sum(),
        pl.col("AMTHKD").sum(), pl.col("AMTAUD").sum(),
    ])

    print("  Writing FISS / NSRS...")
    fiss_path, nsrs_path = output_dir / "FISS.txt", output_dir / "NSRS.txt"
    _write_fiss_nsrs(note_final, ctx, fiss_path, nsrs_path)

    print("  Writing NLF distribution (SUPPL) report...")
    suppl_path = output_dir / "NLF_SUPPL.txt"
    _write_suppl_report(dist_summary, ctx["rdate"], suppl_path)

    print("  Building TOP 100 individual/corporate reports...")
    # SAS DD's for these are scratch (&&INDV/&&CORP, DISP=(NEW,DELETE,DELETE))
    # in the original PIBB job -- persisted here as ordinary files for
    # inspection, since nothing downstream re-reads the transient datasets.
    top_ind, top_org = _build_top100(cisln_deposit_cache, cisdp_deposit_cache, deposit_current_cache, deposit_fd_cache)
    fd11_path, fd12_path = output_dir / "INDV.txt", output_dir / "CORP.txt"
    _write_top100_report(top_ind, "TOP 100 LARGEST FD+CA INDIVIDUAL CUSTOMERS", ctx["rdate"], fd11_path)
    _write_top100_report(top_org, "TOP 100 LARGEST FD+CA CORPORATE CUSTOMERS", ctx["rdate"], fd12_path)

    print("EIIMRLFM complete. Outputs:")
    for p in (fiss_path, nsrs_path, suppl_path, fd11_path, fd12_path):
        print(f"  - {p}")
