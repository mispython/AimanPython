# =============================================================================
# DATE / WEEK DERIVATION  (equivalent of DATA REPTDATE step)
# =============================================================================
# REPTDATE = TODAY() - 1  (SAS: OPTIONS YEARCUTOFF=1950)
reptdate: date = date.today() - timedelta(days=1)

day_of_month = reptdate.day
if 1 <= day_of_month <= 8:
    nowk = 1
elif 9 <= day_of_month <= 15:
    nowk = 2
elif 16 <= day_of_month <= 22:
    nowk = 3
else:
    nowk = 4

# Macro variable equivalents
REPTYEAR = reptdate.strftime("%y")           # 2-digit year  (PUT(REPTDATE,YEAR2.))
REPTMON  = reptdate.strftime("%m")           # zero-padded month (Z2.)
REPTDAY  = reptdate.strftime("%d")           # zero-padded day   (Z2.)
REPTDT   = reptdate.toordinal()              # raw SAS date integer equivalent (used for filter)
RDATE    = reptdate                          # date object used in DATA ECP step
NOWK     = f"{nowk:01d}"                     # zero-padded 1-digit week number (Z1.)
