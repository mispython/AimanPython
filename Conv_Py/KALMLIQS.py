#!/usr/bin/env python3
"""
Program  : KALMLIQS.py
Purpose  : Include fragment — DATA K1TBL and DATA K3TBL steps for New Liquidity
            Framework (KAPITI Part 2 & 3). Processes money market, investment,
            and FX transactions; classifies into KAPITI item codes A2.01-A2.19
            and B2.01-B2.19 by currency and deal type.

           DATE    : 22.07.98
           MODIFIED: 31-12-2001 (WBL) — include interest receivable for
                     investment stock items A2.03 & A2.04 (email 28/12/01)

           This module is NOT a standalone program. It is an %INC PGM(KALMLIQS)
            fragment intended to be called from a parent orchestrating program.
           Import and call build_k1tbl() and build_k3tbl() with required context.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
# $CTYPE. format -> format_ctype() passed as parameter from parent program.
# &NREP, &IREP   -> customer classification lists passed as parameters.
# &INST           -> institution identifier (e.g. 'PBB') passed as parameter.
# &REPTMON, &NOWK -> reporting period identifiers passed as parameters.

import duckdb
import polars as pl
from pathlib import Path
from datetime import date, datetime


# ============================================================================
# KALMLIQS: DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT)
# ============================================================================
# SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWMDT=MATDT GWBALC=AMOUNT))
# IF GWMVT = 'P'
# IF GWOCY='XAU' THEN DELETE
# IF GWCCY='XAU' THEN DELETE
# Classify by GWCCY='MYR' (PART='2-RM') vs else (PART='2-F$')
# ============================================================================

def build_k1tbl(
    bnmk_dir:    Path,
    reptmon:     str,
    nowk:        str,
    nrep:        set,
    irep:        set,
    format_ctype,          # callable: format_ctype(gwctp: str) -> str
) -> pl.DataFrame:
    """
    Replicate DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT) from KALMLIQS.

    Parameters
    ----------
    bnmk_dir     : Directory containing BNMK parquet files.
    reptmon      : Reporting month (Z2. string).
    nowk         : Reporting week key.
    nrep         : Set of CUST values -> item A2.16 (&NREP).
    irep         : Set of CUST values -> item A2.15 (&IREP).
    format_ctype : Function implementing $CTYPE. format.

    Returns
    -------
    pl.DataFrame with columns: PART, ITEM, MATDT, AMOUNT
    """
    k1tbl_file = bnmk_dir / f"k1tbl{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{k1tbl_file}')"
    ).pl()

    # RENAME: GWMDT -> MATDT, GWBALC -> AMOUNT
    rename_map = {}
    if "GWMDT"  in df.columns: rename_map["GWMDT"]  = "MATDT"
    if "GWBALC" in df.columns: rename_map["GWBALC"] = "AMOUNT"
    if rename_map:
        df = df.rename(rename_map)

    rows = []

    for r in df.to_dicts():
        # IF GWMVT = 'P'
        if str(r.get("GWMVT") or "").strip() != "P":
            continue
        # IF GWOCY='XAU' THEN DELETE
        if str(r.get("GWOCY") or "").strip() == "XAU":
            continue
        # IF GWCCY='XAU' THEN DELETE
        gwccy = str(r.get("GWCCY") or "").strip()
        if gwccy == "XAU":
            continue

        gwmvts = str(r.get("GWMVTS") or "").strip()
        gwdlp  = str(r.get("GWDLP")  or "").strip()
        gwctp  = str(r.get("GWCTP")  or "").strip()
        gwshn  = str(r.get("GWSHN")  or "").strip()
        gwact  = str(r.get("GWACT")  or "").strip()
        amount = float(r.get("AMOUNT") or r.get("GWBALC") or 0.0)

        matdt_raw = r.get("MATDT") or r.get("GWMDT")
        if isinstance(matdt_raw, (date, datetime)):
            matdt = matdt_raw.date() if isinstance(matdt_raw, datetime) else matdt_raw
        else:
            matdt = None

        if gwccy == "MYR":
            # ---- PART = '2-RM' ----
            part = "2-RM"

            if gwmvts == "M":
                # IF GWDLP IN ('BCD','BCI','BCS')
                if gwdlp in ("BCD", "BCI", "BCS"):
                    rows.append({"PART": part, "ITEM": "A2.16",
                                 "MATDT": matdt, "AMOUNT": amount})

                # IF SUBSTR(GWCTP,1,1) = 'B'
                if gwctp[:1] == "B":
                    if gwdlp in ("LO","LC","LF","LS","LOI","LSI","LSC","LSW",
                                 "FDA","FDB","FDS","FDL"):
                        amount_abs = abs(amount)
                        rows.append({"PART": part, "ITEM": "A2.01",
                                     "MATDT": matdt, "AMOUNT": amount_abs})
                    elif gwdlp in ("BO","BF","BOI","BFI","BSC","BSW"):
                        rows.append({"PART": part, "ITEM": "A2.14",
                                     "MATDT": matdt, "AMOUNT": amount})

                # SELECT (SUBSTR(GWDLP,2,2))
                gwdlp_sub = gwdlp[1:3] if len(gwdlp) >= 3 else ""
                if gwdlp_sub in ("MI", "MT"):
                    cust = format_ctype(gwctp)
                    item = "A2.15" if cust in irep else "A2.16"
                    rows.append({"PART": part, "ITEM": item,
                                 "MATDT": matdt, "AMOUNT": amount})
                elif gwdlp_sub in ("XI", "XT"):
                    rows.append({"PART": part, "ITEM": "A2.02",
                                 "MATDT": matdt, "AMOUNT": amount})

            elif gwdlp in ("FXS","FXO","FXF","TS1","TS2","SF1","SF2","FF1","FF2"):
                if gwmvts == "P":
                    rows.append({"PART": part, "ITEM": "A2.09",
                                 "MATDT": matdt, "AMOUNT": amount})
                elif gwmvts == "S":
                    rows.append({"PART": part, "ITEM": "A2.19",
                                 "MATDT": matdt, "AMOUNT": amount})

        else:
            # ---- PART = '2-F$' ----
            part = "2-F$"

            if gwmvts == "M":
                if gwctp[:1] == "B":
                    if gwdlp in ("LO","LC","LS","LF","LOI","LSI","LSC","LOC",
                                 "FDA","FDB","FDS","FDL","LOW","LSW"):
                        rows.append({"PART": part, "ITEM": "B2.01",
                                     "MATDT": matdt, "AMOUNT": amount})
                    elif gwdlp in ("BC","BF","BO","BSC","BOC","BOW","BSW"):
                        # IF SUBSTR(GWSHN,1,6) ^= 'FCY-FD'
                        if gwshn[:6] != "FCY-FD":
                            rows.append({"PART": part, "ITEM": "B2.14",
                                         "MATDT": matdt, "AMOUNT": amount})

            elif (gwdlp in ("FXS","FXO","FXF","TS1","TS2","SF1","SF2","FF1","FF2")
                  and gwact not in ("RV", "RW")):
                if gwmvts == "P":
                    rows.append({"PART": part, "ITEM": "B2.09",
                                 "MATDT": matdt, "AMOUNT": amount})
                elif gwmvts == "S":
                    rows.append({"PART": part, "ITEM": "B2.19",
                                 "MATDT": matdt, "AMOUNT": amount})

    if not rows:
        return pl.DataFrame(schema={
            "PART": pl.Utf8, "ITEM": pl.Utf8,
            "MATDT": pl.Date, "AMOUNT": pl.Float64,
        })
    return pl.DataFrame(rows)


# ============================================================================
# KALMLIQS: DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT)
# ============================================================================
# SET BNMK.K3TBL&REPTMON&NOWK
# PART = '2-RM'
# AMOUNT = UTAMOC - UTDPF
# IF UTSTY='IDC' THEN AMOUNT = UTAMOC + UTDPF
# IF UTREF IN ('IAFSLIQ','AFSLIQ','AFSBOND','INV','DRI','DLG','AFS') -> investment types
# ELSE IF UTREF IN ('PFD','PLD','PSD','PZD','PDC')                   -> pledged types
# ELSE IF UTREF IN ('IINV','IDRI','IDLG')                            -> Islamic inv
# IF UTSTY='SIP' -> A2.01 (ABS amount)
# &INST='PBB' -> adjustments to AMOUNT using UTAICT/UTDPEY/UTDPE/UTAICY/UTAIT
# ============================================================================

def build_k3tbl(
    bnmk_dir: Path,
    reptmon:  str,
    nowk:     str,
    inst:     str,         # &INST value, e.g. 'PBB'
) -> pl.DataFrame:
    """
    Replicate DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT) from KALMLIQS.

    Parameters
    ----------
    bnmk_dir : Directory containing BNMK parquet files.
    reptmon  : Reporting month (Z2. string).
    nowk     : Reporting week key.
    inst     : Institution code (&INST), e.g. 'PBB'.

    Returns
    -------
    pl.DataFrame with columns: PART, ITEM, MATDT, AMOUNT
    """
    k3tbl_file = bnmk_dir / f"k3tbl{reptmon}{nowk}.parquet"
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT * FROM read_parquet('{k3tbl_file}')"
    ).pl()

    rows = []

    # UTREF sets for classification
    INV_REFS     = {"IAFSLIQ","AFSLIQ","AFSBOND","INV","DRI","DLG","AFS"}
    PLEDGED_REFS = {"PFD","PLD","PSD","PZD","PDC"}
    ISLM_REFS    = {"IINV","IDRI","IDLG"}

    for r in df.to_dicts():
        utref  = str(r.get("UTREF")  or "").strip()
        utsty  = str(r.get("UTSTY")  or "").strip()
        utdlp  = str(r.get("UTDLP")  or "").strip()
        utamoc = float(r.get("UTAMOC") or 0.0)
        utdpf  = float(r.get("UTDPF")  or 0.0)
        utaict = float(r.get("UTAICT") or 0.0)
        utpcp  = float(r.get("UTPCP")  or 0.0)
        utdpey = float(r.get("UTDPEY") or 0.0)
        utdpe  = float(r.get("UTDPE")  or 0.0)
        utaicy = float(r.get("UTAICY") or 0.0)
        utait  = float(r.get("UTAIT")  or 0.0)

        matdt_raw = r.get("UTMDT") or r.get("MATDT")
        if isinstance(matdt_raw, (date, datetime)):
            matdt = matdt_raw.date() if isinstance(matdt_raw, datetime) else matdt_raw
        else:
            matdt = None

        part   = "2-RM"

        # Base amount calculation
        # AMOUNT = UTAMOC - UTDPF
        # IF UTSTY='IDC' THEN AMOUNT = UTAMOC + UTDPF
        amount = utamoc - utdpf
        if utsty == "IDC":
            amount = utamoc + utdpf

        # ------------------------------------------------------------------
        # IF UTREF IN ('IAFSLIQ','AFSLIQ','AFSBOND','INV','DRI','DLG','AFS')
        # ------------------------------------------------------------------
        if utref in INV_REFS:
            item = None

            if utsty in ("CB1","CB2","CF1","CF2","CNT","MGS","MTB","BNB","BNN",
                         "ITB","SAC","BMN","BMC","BMF","SCD",
                         "CMB","MGI","SMC"):
                item = "A2.03"
                if inst == "PBB":
                    amount = amount + utaict

            elif utsty == "SDC":
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc * (utpcp / 100)) + utdpey + utdpe

            elif utsty == "LDC":
                item = "A2.04"
                if inst == "PBB":
                    amount = amount + utaict

            elif utsty in ("SLD","SSD"):
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc * (utpcp / 100)) + utaicy + utait

            elif utsty in ("SFD","SZD"):
                item = "A2.04"
                if inst == "PBB":
                    amount = amount + utaict

            elif utsty == "SBA":
                # IF UTDLP NOT IN ('MOS','MSS')
                if utdlp not in ("MOS","MSS"):
                    item = "A2.05"

            elif utsty in ("ISB","DHB","KHA"):
                item = "A2.06"

            elif utsty == "DBD":
                item = "A2.07"

            elif utsty in ("DMB","DBD","GRL","MTL","RUL","IBZ","DBZ"):
                item = "A2.08"

            elif utsty == "PBA":
                # IF UTDLP IN ('MOS','MSS')
                if utdlp in ("MOS","MSS"):
                    item = "A2.18"

            if item is not None:
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

        # ------------------------------------------------------------------
        # ELSE IF UTREF IN ('PFD','PLD','PSD','PZD','PDC')
        # ------------------------------------------------------------------
        elif utref in PLEDGED_REFS:
            if utsty in ("IFD","ILD","ISD","IZD","IDC","IDP","IZP"):
                rows.append({"PART": part, "ITEM": "A2.17",
                             "MATDT": matdt, "AMOUNT": amount})

        # ------------------------------------------------------------------
        # ELSE IF UTREF IN ('IINV','IDRI','IDLG')
        # ------------------------------------------------------------------
        elif utref in ISLM_REFS:
            # Re-derive base amount (not modified above for Islamic)
            amount = utamoc - utdpf
            if utsty == "IDC":
                amount = utamoc + utdpf

            if utsty == "SBA" and utdlp == "IOP":
                rows.append({"PART": part, "ITEM": "A2.05",
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty == "SDC":
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc * (utpcp / 100)) + utdpey + utdpe
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

            if utsty == "LDC":
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc - utdpf) + utaict
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty in ("SLD","SSD"):
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc * (utpcp / 100)) + utaicy + utait
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty in ("SFD","SZD"):
                item = "A2.04"
                if inst == "PBB":
                    amount = (utamoc - utdpf) + utaict
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty in ("CB1","CB2","CF1","CF2","CNT","MGI",
                           "ITB","SAC","BMN","BMC","BMF","SCD",
                           "MGS","MTB","BNB","BNN","CMB","SMC"):
                item = "A2.03"
                if inst == "PBB":
                    amount = (utamoc - utdpf) + utaict
                rows.append({"PART": part, "ITEM": item,
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty in ("ISB","DHB","KHA"):
                rows.append({"PART": part, "ITEM": "A2.06",
                             "MATDT": matdt, "AMOUNT": amount})

            elif utsty in ("IBZ","DBZ"):
                rows.append({"PART": part, "ITEM": "A2.08",
                             "MATDT": matdt, "AMOUNT": amount})

        # ------------------------------------------------------------------
        # IF UTSTY='SIP' (applies regardless of UTREF — appended after main SELECT)
        # ------------------------------------------------------------------
        if utsty == "SIP":
            # Re-derive base amount for SIP
            sip_amount = utamoc - utdpf
            rows.append({"PART": part, "ITEM": "A2.01",
                         "MATDT": matdt, "AMOUNT": abs(sip_amount)})

    if not rows:
        return pl.DataFrame(schema={
            "PART": pl.Utf8, "ITEM": pl.Utf8,
            "MATDT": pl.Date, "AMOUNT": pl.Float64,
        })
    return pl.DataFrame(rows)
