# ============================================================================
# FILE: MATDTEX.py  (updated)
# ============================================================================

from datetime import date, timedelta


def _mdy(month: int, day: int, year: int):
    """SAS MDY() function equivalent. Returns None (SAS missing) for an
    out-of-range month/day rather than raising, matching how SAS would
    silently produce a missing date (with a log note) instead of aborting."""
    if month is None or day is None or year is None:
        return None
    if not (1 <= month <= 12):
        return None
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def calc_remmth(reptdate: date, matdt, current_remmth=None):
    """
    Port of the MATDTEX %INC member.

    IMPORTANT -- preservation semantics:
    In the original SAS, this code is textually inserted into
    "DATA LIQCLASS; SET LIQCLASS; ...", i.e. REMMTH is read back in from
    the existing dataset via SET, and the chained
    "IF...THEN REMMTH=01; ELSE IF...THEN REMMTH=02; ... ELSE IF...THEN
    REMMTH=06;" ONLY overwrites it when one of the six band conditions is
    true. If MATDT is missing, or does not fall into any of the six bands
    (e.g. MATDT < DAYA), none of the conditions fire and SAS silently
    KEEPS whatever REMMTH value the row already had (the continuous value
    computed earlier in EIBMLI4I's main DATA step). This function
    reproduces that: pass the row's pre-existing REMMTH in via
    `current_remmth`, and it is returned unchanged whenever no band
    matches -- it is NEVER discarded/blanked out here.

    Args:
        reptdate:       EIBMLI4I's report date (LIQCLASS.REPTDATE column).
        matdt:          The row's maturity date (LIQCLASS.MATDT column).
        current_remmth: The row's REMMTH value as it stood BEFORE this
                         %INC ran (i.e. EIBMLI4I's own REMMTH computation).
                         Returned as-is if no band matches.

    Returns:
        int 1-6 if a band matches, otherwise `current_remmth` unchanged
        (mirrors SAS's SET-then-conditionally-overwrite behaviour).
    """
    if matdt is None or reptdate is None:
        return current_remmth

    daya = reptdate + timedelta(days=1)
    mm0 = daya.month
    yy0 = daya.year
    yy1 = yy0 + 1
    mm1 = mm0 + 1
    mm2 = mm0 + 3
    mm3 = mm0 + 6
    mm4 = mm0 + 12

    if 1 <= mm0 <= 6:
        mm4 -= 12
        dayb = _mdy(mm0, 8, yy0)
        dayc = _mdy(mm1, 1, yy0)
        dayd = _mdy(mm2, 1, yy0)
        daye = _mdy(mm3, 1, yy0)
        dayf = _mdy(mm4, 1, yy1)
    elif 7 <= mm0 <= 9:
        mm4 -= 12
        mm3 -= 12
        dayb = _mdy(mm0, 8, yy0)
        dayc = _mdy(mm1, 1, yy0)
        dayd = _mdy(mm2, 1, yy0)
        daye = _mdy(mm3, 1, yy1)
        dayf = _mdy(mm4, 1, yy1)
    else:  # 10 <= mm0 <= 12
        mm4 -= 12
        mm3 -= 12
        mm2 -= 12
        dayb = _mdy(mm0, 8, yy0)
        if mm1 > 12:
            mm1 -= 12
            dayc = _mdy(mm1, 1, yy1)
        else:
            dayc = _mdy(mm1, 1, yy0)
        dayd = _mdy(mm2, 1, yy1)
        daye = _mdy(mm3, 1, yy1)
        dayf = _mdy(mm4, 1, yy1)

    if None in (dayb, dayc, dayd, daye, dayf):
        # A date computation failed (SAS: comparison against a missing
        # date evaluates false) -- no band condition can fire, so REMMTH
        # is preserved, exactly as SAS would leave it.
        return current_remmth

    if daya <= matdt < dayb:
        return 1
    if dayb <= matdt < dayc:
        return 2
    if dayc <= matdt < dayd:
        return 3
    if dayd <= matdt < daye:
        return 4
    if daye <= matdt < dayf:
        return 5
    if matdt > dayf:
        return 6

    # No condition matched (e.g. MATDT < DAYA) -- SAS keeps the pre-existing
    # REMMTH value from the SET statement, not missing.
    return current_remmth
