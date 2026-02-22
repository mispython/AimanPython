# !/usr/bin/env python3
"""
PROGRAM : MATDTEX
PURPOSE : COMPUTE REMMTH (REMAINING MATURITY BUCKET CODE) FOR EACH RECORD IN LIQCLASS BASED ON REPTDATE
            AND MATDT. USED AS %INC DEPENDENCY BY EIBMLI4I.
          RETURNS AN UPDATED POLARS DATAFRAME.
"""

import polars as pl
import datetime


# ---------------------------------------------------------------------------
# Helper: build a SAS-style date integer from month/day/year components.
# SAS date = days since 01-JAN-1960.
# Returns None if the date is invalid.
# ---------------------------------------------------------------------------
SAS_EPOCH = datetime.date(1960, 1, 1)


def _mdy(mm: int, dd: int, yy: int):
    """Return SAS date integer for MDY(mm,dd,yy), or None on invalid date."""
    try:
        return (datetime.date(yy, mm, dd) - SAS_EPOCH).days
    except ValueError:
        return None


def _year(sas_date: int) -> int:
    d = SAS_EPOCH + datetime.timedelta(days=int(sas_date))
    return d.year


def _month(sas_date: int) -> int:
    d = SAS_EPOCH + datetime.timedelta(days=int(sas_date))
    return d.month


# ---------------------------------------------------------------------------
# MATDTEX logic: compute REMMTH per row
# Inlined from X_MATDTEX — called via %INC PGM(MATDTEX) in EIBMLI4I.
# ---------------------------------------------------------------------------
def _compute_remmth_row(reptdate_sas, matdt_sas) -> int | None:
    """
    Replicate MATDTEX DATA step logic for one row.
    reptdate_sas, matdt_sas : SAS date integers (int).
    Returns REMMTH bucket (1-6) or None if undetermined.

    DATA LIQCLASS;
      SET LIQCLASS;
      DAYA = REPTDATE+1;
      MM0  = MONTH(DAYA);  YY0 = YEAR(DAYA);
      YY1  = YY0+1;
      MM1  = MM0+01; MM2 = MM0+03; MM3 = MM0+06; MM4 = MM0+12;
      ...bucket logic...
    """
    if reptdate_sas is None or matdt_sas is None:
        return None

    daya = int(reptdate_sas) + 1
    mm0  = _month(daya)
    yy0  = _year(daya)
    yy1  = yy0 + 1
    mm1  = mm0 + 1
    mm2  = mm0 + 3
    mm3  = mm0 + 6
    mm4  = mm0 + 12

    matdt = int(matdt_sas)

    if 1 <= mm0 <= 6:
        mm4  = mm4 - 12
        dayb = _mdy(mm0, 8,  yy0)
        dayc = _mdy(mm1, 1,  yy0)
        dayd = _mdy(mm2, 1,  yy0)
        daye = _mdy(mm3, 1,  yy0)
        dayf = _mdy(mm4, 1,  yy1)
    elif 7 <= mm0 <= 9:
        mm4  = mm4 - 12
        mm3  = mm3 - 12
        dayb = _mdy(mm0, 8,  yy0)
        dayc = _mdy(mm1, 1,  yy0)
        dayd = _mdy(mm2, 1,  yy0)
        daye = _mdy(mm3, 1,  yy1)
        dayf = _mdy(mm4, 1,  yy1)
    elif 10 <= mm0 <= 12:
        mm4  = mm4 - 12
        mm3  = mm3 - 12
        mm2  = mm2 - 12
        dayb = _mdy(mm0, 8, yy0)
        if mm1 > 12:
            mm1  = mm1 - 12
            dayc = _mdy(mm1, 1, yy1)
        else:
            dayc = _mdy(mm1, 1, yy0)
        dayd = _mdy(mm2, 1, yy1)
        daye = _mdy(mm3, 1, yy1)
        dayf = _mdy(mm4, 1, yy1)
    else:
        return None

    # Bucket assignment
    if dayb is not None and daya <= matdt < dayb:
        return 1
    elif dayb is not None and dayc is not None and dayb <= matdt < dayc:
        return 2
    elif dayc is not None and dayd is not None and dayc <= matdt < dayd:
        return 3
    elif dayd is not None and daye is not None and dayd <= matdt < daye:
        return 4
    elif daye is not None and dayf is not None and daye <= matdt < dayf:
        return 5
    elif dayf is not None and matdt > dayf:
        return 6
    return None


def apply_matdtex(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply MATDTEX logic to a Polars DataFrame that contains
    REPTDATE (SAS date int) and MATDT (SAS date int) columns.
    Adds / overwrites the REMMTH column.
    """
    reptdate_vals = df["REPTDATE"].to_list()
    matdt_vals    = df["MATDT"].to_list()

    remmth_vals = [
        _compute_remmth_row(r, m)
        for r, m in zip(reptdate_vals, matdt_vals)
    ]

    return df.with_columns(pl.Series("REMMTH", remmth_vals, dtype=pl.Float64))
