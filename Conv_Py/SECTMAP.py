#!/usr/bin/env python3
"""
Program : SECTMAP.py
Purpose : Sector code mapping and hierarchy expansion for ALM dataset.
          Designed to be imported (%INC equivalent) by orchestrator programs.
          Applies NEWSECT / VALIDSE format lookups to normalise SECTORCD,
              then expands each sector code into its full parent-rollup chain
              (ALM2), and finally aggregates to top-level single-digit sector
              groups (ALMA).

Note    : The original SAS program does not %INC PBBLNFMT. The $NEWSECT.
              and $VALIDSE. formats are assumed to be available in the SAS
              session (loaded separately by the orchestrator).
          Accordingly, this Python module does NOT import from PBBLNFMT;
              instead the equivalent format logic is inlined below as
              private helpers, consistent with how SAS resolves formats at runtime.

Usage (orchestrator) :
  from SECTMAP import process_sectmap
  alm_df = process_sectmap(alm_df)   # returns polars DataFrame
"""

from __future__ import annotations

from typing import Dict, List, Tuple
import polars as pl

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
# Input / output paths are managed by the calling orchestrator program.
# SECTMAP is a logic module; it operates on DataFrames passed in by reference.
# ---------------------------------------------------------------------------


# =============================================================================
# INLINE FORMAT EQUIVALENTS
#   The SAS session makes $NEWSECT. and $VALIDSE. available via PROC FORMAT
#   (defined in PBBLNFMT). Since this program does not %INC PBBLNFMT, the
#   equivalent logic is reproduced here as private module-level helpers.
# =============================================================================

# ----------------------------------------------------------------------------
# $NEWSECT. – new sector code normalisation map
# ----------------------------------------------------------------------------
def _build_newsect_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for k in ['1111', '1113', '1115', '1117', '1119', '1200', '1300', '1400',
              '2100', '2301', '2303', '2400', '2900',
              '3115', '3113', '3114', '3120', '3250', '3271', '3272', '3273',
              '3280', '3290', '3825', '3710', '3721', '3731', '3732',
              '3811', '3813', '3814', '3819', '3833', '3834', '3835',
              '3911', '3919', '3952', '3953', '3955', '3956', '3957', '3960',
              '5001', '5002', '5003', '5004', '5005', '5006', '5008',
              '5020', '5030', '5040', '5050', '5999',
              '6110', '6120', '6130',
              '7111', '7114', '7116', '7117', '7122', '7124', '7131', '7132',
              '7133', '7134', '7191', '7192', '7193', '7199', '7210', '7220',
              '8310', '8321', '8331', '8333', '8340',
              '8411', '8412', '8413', '8414', '8415', '8416', '8420',
              '8911', '8912', '8913', '8921', '8931', '8932', '8991', '8999',
              '9431', '9433', '9434', '9500', '9600', '9999']:
        m[k] = k
    for k in ['2210', '2220']:
        m[k] = '2200'
    for k in ['3211', '3212', '3219']:
        m[k] = '3210'
    for k in ['3221', '3222']:
        m[k] = '3220'
    for k in ['3231', '3232']:
        m[k] = '3230'
    for k in ['3241', '3242']:
        m[k] = '3240'
    for k in ['3311', '3312', '3313']:
        m[k] = '3310'
    for k in ['3431', '3432', '3433']:
        m[k] = '3430'
    for k in ['3551', '3552']:
        m[k] = '3550'
    for k in ['3611', '3619']:
        m[k] = '3610'
    for k in ['3842', '3843', '3844']:
        m[k] = '3841'
    for k in ['3851', '3852', '3853']:
        m[k] = '3850'
    for k in ['3861', '3862', '3863', '3864', '3865', '3866']:
        m[k] = '3860'
    for k in ['3871', '3872', '3873']:
        m[k] = '3870'
    for k in ['3891', '3892', '3893', '3894']:
        m[k] = '3890'
    for k in ['4010', '4020', '4030']:
        m[k] = '4000'
    for k in ['6310', '6320']:
        m[k] = '6300'
    for k in ['8110', '8120', '8130']:
        m[k] = '8100'
    for k in ['9101', '9102', '9103']:
        m[k] = '9100'
    for k in ['9201', '9202', '9203']:
        m[k] = '9200'
    for k in ['9311', '9312', '9313', '9314']:
        m[k] = '9300'
    return m


_NEWSECT_MAP: Dict[str, str] = _build_newsect_map()


def _newsect(code: str) -> str:
    """Equivalent of PUT(code, $NEWSECT.) – returns '' when no match."""
    return _NEWSECT_MAP.get(code.strip(), '')


# ----------------------------------------------------------------------------
# $VALIDSE. – valid sector code check
# ----------------------------------------------------------------------------
_VALIDSE_SET: frozenset = frozenset({
    '1111', '1112', '1113', '1114', '1115', '1116', '1117', '1119',
    '1120', '1130', '1140', '1150', '1200', '1300', '1400',
    '2100', '2210', '2220', '2301', '2302', '2303', '2400', '2900',
    '3110', '3111', '3112', '3113', '3114', '3115', '3120',
    '3211', '3212', '3219', '3221', '3222', '3231', '3232',
    '3241', '3242', '3250', '3271', '3272', '3273', '3280', '3290',
    '3311', '3312', '3313', '3431', '3432', '3433', '3551', '3552',
    '3611', '3619', '3710', '3720', '3721', '3731', '3732',
    '3811', '3813', '3814', '3819', '3825',
    '3832', '3833', '3834', '3835',
    '3842', '3843', '3844', '3851', '3852', '3853',
    '3861', '3862', '3863', '3864', '3865', '3866',
    '3871', '3872', '3873', '3891', '3892', '3893', '3894',
    '3911', '3919', '3952', '3953', '3955', '3956', '3957', '3960',
    '4010', '4020', '4030',
    '5001', '5002', '5003', '5004', '5005', '5006', '5008',
    '5020', '5030', '5040', '5050', '5999',
    '6110', '6120', '6130', '6310', '6320',
    '7111', '7112', '7113', '7114', '7115', '7116', '7117',
    '7121', '7122', '7123', '7124',
    '7131', '7132', '7133', '7134',
    '7191', '7192', '7193', '7199', '7210', '7220',
    '8110', '8120', '8130',
    '8310', '8320', '8321', '8331', '8332', '8333', '8340',
    '8411', '8412', '8413', '8414', '8415', '8416', '8420',
    '8910', '8911', '8912', '8913', '8914',
    '8920', '8921', '8922', '8931', '8932', '8991', '8999',
    '9101', '9102', '9103', '9201', '9202', '9203',
    '9311', '9312', '9313', '9314',
    '9410', '9420', '9430', '9431', '9432', '9433', '9434', '9435',
    '9440', '9450', '9499', '9500', '9600', '9700', '9999',
})


def _validse(code: str) -> str:
    """Equivalent of PUT(code, $VALIDSE.) – returns 'VALID' or 'INVALID'."""
    return 'VALID' if code.strip() in _VALIDSE_SET else 'INVALID'


# =============================================================================
# STEP 1 – apply_newsect_validse
#   Replicates the first two DATA ALM steps:
#     1. Derive SECTA  via PUT(SECTORCD, $NEWSECT.)
#        and SECVALID  via PUT(SECTORCD, $VALIDSE.)
#     2. Assign SECTCD = SECTA if SECTA is non-blank, else SECTORCD.
#     3. For INVALID codes apply prefix-based fallback mapping.
# =============================================================================

# Prefix-based fallback rules applied when SECVALID = 'INVALID'.
# Each entry: (prefix_len, prefix_value, replacement_sectcd).
# Rules are evaluated in the order listed, matching the SAS IF sequence.
_INVALID_FALLBACK: List[Tuple[int, str, str]] = [
    (1, '1',  '1400'),
    (1, '2',  '2900'),
    (1, '3',  '3919'),
    (1, '4',  '4010'),
    (1, '5',  '5999'),
    (2, '61', '6120'),
    (2, '62', '6130'),
    (2, '63', '6310'),
    (2, '64', '6130'),
    (2, '65', '6130'),
    (2, '66', '6130'),
    (2, '67', '6130'),
    (2, '68', '6130'),
    (2, '69', '6130'),
    (1, '7',  '7199'),
    (2, '81', '8110'),
    (2, '82', '8110'),
    (2, '83', '8999'),
    (2, '84', '8999'),
    (2, '85', '8999'),
    (2, '86', '8999'),
    (2, '87', '8999'),
    (2, '88', '8999'),
    (2, '89', '8999'),
    (2, '91', '9101'),
    (2, '92', '9410'),
    (2, '93', '9499'),
    (2, '94', '9499'),
    (2, '95', '9499'),
    (2, '96', '9999'),
    (2, '97', '9999'),
    (2, '98', '9999'),
    (2, '99', '9999'),
]


def _apply_invalid_fallback(sectcd: str) -> str:
    """Return fallback SECTCD based on leading prefix when code is INVALID."""
    for prefix_len, prefix_val, replacement in _INVALID_FALLBACK:
        if sectcd[:prefix_len] == prefix_val:
            return replacement
    return sectcd


def apply_newsect_validse(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA ALM steps 1 & 2.
    Expects column SECTORCD (str).
    Returns DataFrame with SECTCD column added/updated.
    """
    secta_list:    List[str] = []
    secvalid_list: List[str] = []
    sectcd_list:   List[str] = []

    for sectorcd in df["SECTORCD"].to_list():
        raw      = (sectorcd or "").strip()
        secta    = _newsect(raw)
        secvalid = _validse(raw)
        secta_list.append(secta)
        secvalid_list.append(secvalid)

        # SECTCD = SECTA if non-blank, else original SECTORCD
        sectcd = secta if secta.strip() != "" else raw

        # Apply prefix-based fallback for INVALID codes
        if secvalid == "INVALID":
            sectcd = _apply_invalid_fallback(sectcd)

        sectcd_list.append(sectcd)

    return df.with_columns([
        pl.Series("SECTA",    secta_list),
        pl.Series("SECVALID", secvalid_list),
        pl.Series("SECTCD",   sectcd_list),
    ])


# =============================================================================
# STEP 2 – expand_alm2
#   Replicates DATA ALM2 SET ALM.
#   Each SECTCD value can emit one or more rows (via SAS OUTPUT) where
#   SECTORCD is replaced by a parent rollup value.
#   Rows that produce no OUTPUT are absent from ALM2.
# =============================================================================

def _expand_row(sectcd: str) -> List[str]:
    """
    Return the list of SECTORCD values that DATA ALM2 would OUTPUT for a
    given SECTCD value, in the same sequence as the SAS program.
    Returns an empty list when SECTCD matches no IF block.
    """
    outputs: List[str] = []

    # ---- 1100 block ----
    if sectcd in ('1111', '1112', '1113', '1114', '1115', '1116',
                  '1117', '1119', '1120', '1130', '1140', '1150'):
        if sectcd in ('1111', '1113', '1115', '1117', '1119'):
            outputs.append('1110')
        outputs.append('1100')

    # ---- 2200 block ----
    if sectcd in ('2210', '2220'):
        outputs.append('2200')

    # ---- 2300 block ----
    if sectcd in ('2301', '2302', '2303'):
        if sectcd in ('2301', '2302'):
            outputs.append('2300')
        outputs.append('2300')
        if sectcd == '2303':
            outputs.append('2302')

    # ---- 3100 / 3110 block ----
    if sectcd in ('3110', '3115', '3111', '3112', '3113', '3114'):
        if sectcd in ('3110', '3113', '3114'):
            outputs.append('3100')
        if sectcd in ('3115', '3111', '3112'):
            outputs.append('3110')

    # ---- 3210 block ----
    if sectcd in ('3211', '3212', '3219'):
        outputs.append('3210')

    # ---- 3220 block ----
    if sectcd in ('3221', '3222'):
        outputs.append('3220')

    # ---- 3230 block ----
    if sectcd in ('3231', '3232'):
        outputs.append('3230')

    # ---- 3240 block ----
    if sectcd in ('3241', '3242'):
        outputs.append('3240')

    # ---- 3260 / 3270 / 3310 block ----
    if sectcd in ('3270', '3280', '3290', '3271', '3272', '3273',
                  '3311', '3312', '3313'):
        if sectcd in ('3270', '3280', '3290', '3271', '3272', '3273'):
            outputs.append('3260')
        if sectcd in ('3271', '3272', '3273'):
            outputs.append('3270')
        if sectcd in ('3311', '3312', '3313'):
            outputs.append('3310')

    # ---- 3430 block ----
    if sectcd in ('3431', '3432', '3433'):
        outputs.append('3430')

    # ---- 3550 block ----
    if sectcd in ('3551', '3552'):
        outputs.append('3550')

    # ---- 3610 block ----
    if sectcd in ('3611', '3619'):
        outputs.append('3610')

    # ---- 3700 block ----
    if sectcd in ('3710', '3720', '3730', '3720', '3721', '3731', '3732'):
        outputs.append('3700')
        if sectcd == '3721':
            outputs.append('3720')
        if sectcd in ('3731', '3732'):
            outputs.append('3730')

    # ---- 3800 block ----
    if sectcd in ('3811', '3812'):
        outputs.append('3800')

    if sectcd in ('3813', '3814', '3819'):
        outputs.append('3812')

    # ---- 3831 / 3832 block ----
    if sectcd in ('3832', '3834', '3835', '3833'):
        outputs.append('3831')
        if sectcd == '3833':
            outputs.append('3832')

    # ---- 3841 block ----
    if sectcd in ('3842', '3843', '3844'):
        outputs.append('3841')

    # ---- 3850 block ----
    if sectcd in ('3851', '3852', '3853'):
        outputs.append('3850')

    # ---- 3860 block ----
    if sectcd in ('3861', '3862', '3863', '3864', '3865', '3866'):
        outputs.append('3860')

    # ---- 3870 block ----
    if sectcd in ('3871', '3872', '3872'):  # SAS has '3872' twice – preserved
        outputs.append('3870')

    # ---- 3890 block ----
    if sectcd in ('3891', '3892', '3893', '3894'):
        outputs.append('3890')

    # ---- 3910 block ----
    if sectcd in ('3911', '3919'):
        outputs.append('3910')

    # ---- 3950 / 3951 / 3954 block ----
    if sectcd in ('3951', '3952', '3953', '3954', '3955', '3956', '3957'):
        outputs.append('3950')
        if sectcd in ('3952', '3953'):
            outputs.append('3951')
        if sectcd in ('3955', '3956', '3957'):
            outputs.append('3954')

    # ---- 5010 block ----
    if sectcd in ('5001', '5002', '5003', '5004', '5005', '5006', '5008'):
        outputs.append('5010')

    # ---- 6100 block ----
    if sectcd in ('6110', '6120', '6130'):
        outputs.append('6100')

    # ---- 6300 block ----
    if sectcd in ('6310', '6320'):
        outputs.append('6300')

    # ---- 7110 / 7112 / 7113 / 7115 block ----
    if sectcd in ('7111', '7112', '7117', '7113', '7114', '7115', '7116'):
        outputs.append('7110')
    if sectcd in ('7113', '7114', '7115', '7116'):
        outputs.append('7112')
    if sectcd in ('7112', '7114'):
        outputs.append('7113')
    if sectcd == '7116':
        outputs.append('7115')

    # ---- 7120 / 7123 / 7121 block ----
    if sectcd in ('7121', '7122', '7123', '7124'):
        outputs.append('7120')
        if sectcd == '7124':
            outputs.append('7123')
        if sectcd == '7122':
            outputs.append('7121')

    # ---- 7130 block ----
    if sectcd in ('7131', '7132', '7133', '7134'):
        outputs.append('7130')

    # ---- 7190 block ----
    if sectcd in ('7191', '7192', '7193', '7199'):
        outputs.append('7190')

    # ---- 7200 block ----
    if sectcd in ('7210', '7220'):
        outputs.append('7200')

    # ---- 8100 block ----
    if sectcd in ('8110', '8120', '8130'):
        outputs.append('8100')

    # ---- 8300 / 8330 block ----
    if sectcd in ('8310', '8330', '8340', '8320', '8331', '8332'):
        outputs.append('8300')
        if sectcd in ('8320', '8331', '8332'):
            outputs.append('8330')

    if sectcd == '8321':
        outputs.append('8320')

    if sectcd == '8333':
        outputs.append('8332')

    # ---- 8400 / 8410 block ----
    if sectcd in ('8420', '8411', '8412', '8413', '8414', '8415', '8416'):
        outputs.append('8400')
        if sectcd in ('8411', '8412', '8413', '8414', '8415', '8416'):
            outputs.append('8410')

    # ---- 8900 / 89xx blocks ----
    if sectcd[:2] == '89':
        outputs.append('8900')
        if sectcd in ('8910', '8911', '8912', '8913', '8914'):
            if sectcd in ('8911', '8912', '8913', '8914'):
                outputs.append('8910')
            if sectcd == '8910':
                outputs.append('8914')
        if sectcd in ('8921', '8922', '8920'):
            if sectcd in ('8921', '8922'):
                outputs.append('8920')
            if sectcd == '8920':
                outputs.append('8922')
        if sectcd in ('8931', '8932'):
            outputs.append('8930')
        if sectcd in ('8991', '8999'):
            outputs.append('8990')

    # ---- 9100 block ----
    if sectcd in ('9101', '9102', '9103'):
        outputs.append('9100')

    # ---- 9200 block ----
    if sectcd in ('9201', '9202', '9203'):
        outputs.append('9200')

    # ---- 9300 block ----
    if sectcd in ('9311', '9312', '9313', '9314'):
        outputs.append('9300')

    # ---- 9400 / 9430 / 9499 block ----
    if sectcd[:2] == '94':
        outputs.append('9400')
        if sectcd in ('9433', '9434', '9435', '9432', '9431'):
            if sectcd in ('9433', '9434', '9435'):
                outputs.append('9432')
            outputs.append('9430')
        if sectcd in ('9410', '9420', '9440', '9450'):
            outputs.append('9499')

    return outputs


def expand_alm2(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA ALM2 SET ALM.
    For each row, produce zero-or-more expansion rows where SECTORCD is
    replaced by the parent rollup value.
    Rows that produce no OUTPUT in SAS are absent from the result.
    Returns the expanded DataFrame (equivalent to dataset ALM2).
    """
    rows: List[dict] = []
    base_cols = df.columns
    records   = df.to_dicts()

    for rec in records:
        sectcd = (rec.get("SECTCD") or "").strip()
        for sectorcd_val in _expand_row(sectcd):
            new_rec = dict(rec)
            new_rec["SECTORCD"] = sectorcd_val
            rows.append(new_rec)

    if not rows:
        return df.clear()

    return pl.from_dicts(rows, schema={c: df[c].dtype for c in base_cols})


# =============================================================================
# STEP 3 – merge_alm_alm2
#   Replicates:
#     DATA ALM;
#       SET ALM(IN=A) ALM2;
#       IF A THEN SECTORCD = SECTCD;
#   Stack original ALM (SECTORCD overridden to SECTCD) on top of ALM2 rows.
# =============================================================================

def merge_alm_alm2(alm: pl.DataFrame, alm2: pl.DataFrame) -> pl.DataFrame:
    """
    Combine original ALM rows (SECTORCD := SECTCD) with ALM2 expansion rows.
    """
    alm_updated = alm.with_columns(
        pl.col("SECTCD").alias("SECTORCD")
    )
    return pl.concat([alm_updated, alm2], how="diagonal")


# =============================================================================
# STEP 4 – expand_alma
#   Replicates DATA ALMA SET ALM.
#   Each SECTORCD belonging to a top-level group emits one new row with
#   SECTORCD replaced by the single-digit group code (1000–9000).
#   Rows whose SECTORCD does not match any group are suppressed.
# =============================================================================

_ALMA_MAP: Dict[str, str] = {}
_ALMA_GROUPS: List[Tuple[str, List[str]]] = [
    ('1000', ['1100', '1200', '1300', '1400']),
    ('2000', ['2100', '2200', '2300', '2400', '2900']),
    ('3000', ['3100', '3120', '3210', '3220', '3230', '3240',
              '3250', '3260', '3310', '3430', '3550', '3610',
              '3700', '3800', '3825', '3831', '3841', '3850',
              '3860', '3870', '3890', '3910', '3950', '3960']),
    ('4000', ['4010', '4020', '4030']),
    ('5000', ['5010', '5020', '5030', '5040', '5050', '5999']),
    ('6000', ['6100', '6300']),
    ('7000', ['7110', '7120', '7130', '7190', '7200']),
    ('8000', ['8100', '8300', '8400', '8900']),
    ('9000', ['9100', '9200', '9300', '9400', '9500', '9600']),
]
for _group_code, _members in _ALMA_GROUPS:
    for _m in _members:
        _ALMA_MAP[_m] = _group_code


def expand_alma(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replicate DATA ALMA SET ALM.
    Emit one row per matching SECTORCD with the group-level SECTORCD value.
    Rows without a matching group are suppressed.
    """
    rows: List[dict] = []
    records = df.to_dicts()

    for rec in records:
        sectorcd = (rec.get("SECTORCD") or "").strip()
        group = _ALMA_MAP.get(sectorcd)
        if group is not None:
            new_rec = dict(rec)
            new_rec["SECTORCD"] = group
            rows.append(new_rec)

    if not rows:
        return df.clear()

    return pl.from_dicts(rows, schema={c: df[c].dtype for c in df.columns})


# =============================================================================
# STEP 5 – finalise_alm
#   Replicates:
#     DATA ALM;
#       SET ALM ALMA;
#       IF SECTORCD EQ '    ' THEN SECTORCD = '9999';
# =============================================================================

def finalise_alm(alm: pl.DataFrame, alma: pl.DataFrame) -> pl.DataFrame:
    """
    Stack ALM and ALMA, then replace blank SECTORCD with '9999'.
    """
    combined = pl.concat([alm, alma], how="diagonal")
    combined = combined.with_columns(
        pl.when(pl.col("SECTORCD").str.strip_chars() == "")
        .then(pl.lit("9999"))
        .otherwise(pl.col("SECTORCD"))
        .alias("SECTORCD")
    )
    return combined


# =============================================================================
# MAIN ENTRY POINT
#   process_sectmap(alm_df) – orchestrators call this single function.
#   Returns the fully processed ALM DataFrame equivalent to the final
#   DATA ALM / ALMA datasets stacked together as produced by the SAS program.
# =============================================================================

def process_sectmap(alm_df: pl.DataFrame) -> pl.DataFrame:
    """
    Full SECTMAP processing pipeline.

    Parameters
    ----------
    alm_df : pl.DataFrame
        Input ALM dataset with at minimum a SECTORCD (str) column.

    Returns
    -------
    pl.DataFrame
        Final combined ALM + ALMA dataset with normalised and expanded
        SECTORCD hierarchy, matching the SAS SECTMAP output.
    """
    # Step 1 – normalise SECTORCD -> SECTCD using inlined $NEWSECT. / $VALIDSE.
    alm = apply_newsect_validse(alm_df)

    # Step 2 – produce ALM2 (hierarchy expansion rows)
    alm2 = expand_alm2(alm)

    # Step 3 – merge original ALM (SECTORCD := SECTCD) with ALM2
    alm = merge_alm_alm2(alm, alm2)

    # Step 4 – produce ALMA (top-level group aggregation rows)
    alma = expand_alma(alm)

    # Step 5 – stack ALM + ALMA, fill blank SECTORCD with '9999'
    result = finalise_alm(alm, alma)

    return result
