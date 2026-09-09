# ============================================================================
# FILE: MATDTEX.py
# PURPOSE: %INC PGM(MATDTEX) member -- reproduces the BNM remaining-maturity
#          band classification (REMMTH = 1..6) used by EIBMLI4I.
#
# Original SAS (source-library member, textually inserted at the %INC point
# inside EIBMLI4I's own DATA LIQCLASS step -- NOT a macro definition, and NOT
# a runtime data input):
#
#   DATA LIQCLASS;
#     SET LIQCLASS;
#     DAYA =REPTDATE+1;
#     MM0  =MONTH(DAYA);  YY0=YEAR(DAYA);  YY1=YY0+1;
#     MM1=MM0+01; MM2=MM0+03; MM3=MM0+06; MM4=MM0+12;
#     IF (01<=MM0<=06) THEN DO ... END;
#     IF (07<=MM0<=09) THEN DO ... END;
#     IF (10<=MM0<=12) THEN DO ... END;
#     IF (DAYA<=MATDT<DAYB) THEN REMMTH=01; ELSE ... ELSE
#     IF (MATDT>DAYF) THEN REMMTH=06;
#   RUN;
#
# This %INC executes AFTER EIBMLI4I's PROC SORT and OVERWRITES the earlier,
# continuous REMMTH value (ROUND((TSM/365)*12,.01)) computed in EIBMLI4I's
# main DATA step, with this 1..6 band classification. EIBMLI4I's later
# "MRNGE=PUT(REMMTH,REMFMT.);" formats THIS value, not the earlier one.
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


def calc_remmth(reptdate: date, matdt):
    """
    Port of the MATDTEX %INC member.

    Args:
        reptdate: EIBMLI4I's report date (LIQCLASS.REPTDATE column -- the
                   value derived from DATA REPTDATE / UTRPT).
        matdt:    The row's maturity date (LIQCLASS.MATDT column).

    Returns:
        int 1-6 maturity-band classification, or None if MATDT does not
        fall in any of the six bands (mirrors SAS leaving REMMTH missing
        when none of the chained IF/ELSE IF conditions are true).
    """
    if matdt is None or reptdate is None:
        return None

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
        return None

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
    return None
