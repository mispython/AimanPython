# !/usr/bin/env python3
"""
Program : LALQPBBP
Purpose : REPORT ON DOMESTIC ASSETS AND LIABILITIES - PART III
          Loan by State/Purpose, Loan by State/Sector, Fixed/Floating Rate Loans
          Final consolidation and PROC PRINT report

%INC PGM(PBBLNFMT);
Placeholder for PBBLNFMT dependency (format definitions: $NEWSECT., $VALIDSE., ODRATE., LNRATE.)
PBBLNFMT defines:
  $NEWSECT. - maps SECTORCD to new sector codes
  $VALIDSE. - validates sector codes (returns 'INVALID' if invalid)
  ODRATE.   - maps PRODUCT to BIC code for OD/overdraft rate
  LNRATE.   - maps PRODUCT to BIC code for loan rate
"""

import duckdb
import polars as pl
import os

import PBBLNFMT

# ─────────────────────────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR     = os.path.join(BASE_DIR, "input")
OUTPUT_DIR    = os.path.join(BASE_DIR, "output")

# Macro variables (equivalent to &REPTMON, &NOWK, &RDATE, &SDESC)
REPTMON  = "202412"
NOWK     = "01"
RDATE    = "31122024"
SDESC    = "REPORT ON DOMESTIC ASSETS AND LIABILITIES"

LOAN_PARQUET = os.path.join(INPUT_DIR, f"LOAN{REPTMON}{NOWK}.parquet")
LALQ_PARQUET = os.path.join(OUTPUT_DIR, f"LALQ{REPTMON}{NOWK}.parquet")
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, f"X_LALQPBBP_{REPTMON}{NOWK}.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
PAGE_LENGTH = 60  # lines per page (ASA carriage control)

# ─────────────────────────────────────────────────────────────
# FORMAT PLACEHOLDERS
# Normally loaded from PBBLNFMT; placeholder mappings below.
# ─────────────────────────────────────────────────────────────

# $NEWSECT. - sector remapping (populated from PBBLNFMT in production)
NEWSECT_MAP: dict = {}   # {old_sectorcd: new_sectorcd}

# $VALIDSE. - sector validity (populated from PBBLNFMT in production)
VALIDSE_MAP: dict = {}   # {sectorcd: 'VALID' or 'INVALID'}

# ODRATE. - product -> BIC for OD products
ODRATE_MAP: dict = {}    # {product_code: bic_string}

# LNRATE. - product -> BIC for loan products
LNRATE_MAP: dict = {}    # {product_code: bic_string}


def newsect_format(sectorcd: str) -> str:
    """Apply $NEWSECT. format - returns mapped sector or '    ' (4 spaces) if not found."""
    return NEWSECT_MAP.get(sectorcd, "    ")


def validse_format(sectorcd: str) -> str:
    """Apply $VALIDSE. format - returns 'INVALID' if not in valid set, else ''."""
    return VALIDSE_MAP.get(sectorcd, "")


def odrate_format(product) -> str:
    """Apply ODRATE. format - returns BIC string or ' ' if not found."""
    return ODRATE_MAP.get(str(product), " ")


def lnrate_format(product) -> str:
    """Apply LNRATE. format - returns BIC string or ' ' if not found."""
    return LNRATE_MAP.get(str(product), " ")


# ─────────────────────────────────────────────────────────────
# PROC DATASETS: DELETE LALQ{REPTMON}{NOWK}
# ─────────────────────────────────────────────────────────────
if os.path.exists(LALQ_PARQUET):
    os.remove(LALQ_PARQUET)

# ─────────────────────────────────────────────────────────────
# READ LOAN INPUT AND APPLY PAIDIND / EIR_ADJ FILTER
# WHERE PAIDIND NOT IN ('P','C') OR EIR_ADJ NE .
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
df_loan_raw = con.execute(f"""
    SELECT * FROM read_parquet('{LOAN_PARQUET}')
    WHERE PAIDIND NOT IN ('P','C') OR EIR_ADJ IS NOT NULL
""").pl()
con.close()


# ─────────────────────────────────────────────────────────────
# HELPER: append to LALQ parquet (equivalent to PROC APPEND)
# ─────────────────────────────────────────────────────────────
def append_to_lalq(df: pl.DataFrame):
    """Append df to LALQ parquet (create if not exists)."""
    if os.path.exists(LALQ_PARQUET):
        con = duckdb.connect()
        existing = con.execute(f"SELECT * FROM read_parquet('{LALQ_PARQUET}')").pl()
        con.close()
        combined = pl.concat([existing, df], how="diagonal")
    else:
        combined = df
    combined.write_parquet(LALQ_PARQUET)


# ─────────────────────────────────────────────────────────────
# SECTION 1: LOAN - BY STATE CODE AND BY PURPOSE CODE
# PROC SUMMARY: CLASS FISSPURP STATECD AMTIND; WHERE PRODCD starts with '34'
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
con.register("loan_tbl", df_loan_raw)
alq = con.execute("""
    SELECT FISSPURP, STATECD, AMTIND,
           SUM(BAL_AFT_EIR) AS AMOUNT
    FROM loan_tbl
    WHERE SUBSTR(PRODCD, 1, 2) = '34'
    GROUP BY FISSPURP, STATECD, AMTIND
""").pl()
con.close()

# DATA ALQ1: remap FISSPURP
alq1_records = []
for row in alq.iter_rows(named=True):
    if row["FISSPURP"] in ('0220', '0230', '0210', '0211', '0212'):
        new_row = dict(row)
        new_row["FISSPURP"] = '0200'
        alq1_records.append(new_row)

alq1 = pl.DataFrame(alq1_records, schema=alq.schema) if alq1_records else pl.DataFrame(schema=alq.schema)

# DATA ALQ: SET ALQ ALQ1
alq = pl.concat([alq, alq1], how="diagonal")

# DATA ALQLOAN: KEEP=BNMCODE AMTIND AMOUNT
alqloan_records = []
for row in alq.iter_rows(named=True):
    fisspurp = str(row.get("FISSPURP", "") or "")
    statecd  = str(row.get("STATECD",  "") or "")
    amtind   = str(row.get("AMTIND",   "") or "")
    amount   = row.get("AMOUNT", 0) or 0
    bnmcode  = ('340000000' + fisspurp + statecd)[:14]
    alqloan_records.append({"BNMCODE": bnmcode, "AMTIND": amtind, "AMOUNT": amount})

alqloan = pl.DataFrame(
    alqloan_records,
    schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64},
)

# PROC APPEND DATA=ALQLOAN BASE=BNM.LALQ{REPTMON}{NOWK}
append_to_lalq(alqloan)
# PROC DATASETS: DELETE ALQ ALQLOAN (handled by scope)


# ─────────────────────────────────────────────────────────────
# SECTION 2: LOAN - BY STATE CODE AND BY SECTOR CODE
# WHERE PRODCD starts with '34' AND SECTORCD NE '0410'
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
con.register("loan_tbl", df_loan_raw)
alq = con.execute("""
    SELECT SECTORCD, STATECD, AMTIND,
           SUM(BAL_AFT_EIR) AS AMOUNT
    FROM loan_tbl
    WHERE SUBSTR(PRODCD, 1, 2) = '34'
      AND SECTORCD <> '0410'
    GROUP BY SECTORCD, STATECD, AMTIND
""").pl()
con.close()

# ── NEW SECTOR MAPPING ──
def apply_sector_mapping(alq: pl.DataFrame) -> pl.DataFrame:
    """Apply $NEWSECT. and $VALIDSE. formats and invalid sector fallbacks."""
    records = []
    for row in alq.iter_rows(named=True):
        sectorcd = str(row.get("SECTORCD", "") or "")
        statecd  = str(row.get("STATECD",  "") or "")
        amtind   = str(row.get("AMTIND",   "") or "")
        amount   = row.get("AMOUNT", 0) or 0

        secta    = newsect_format(sectorcd)
        secvalid = validse_format(sectorcd)

        sectcd = secta if secta.strip() != '' else sectorcd

        # Apply invalid sector fallback
        if secvalid == 'INVALID':
            if sectcd[:1] == '1':
                sectcd = '1400'
            elif sectcd[:1] == '2':
                sectcd = '2900'
            elif sectcd[:1] == '3':
                sectcd = '3919'
            elif sectcd[:1] == '4':
                sectcd = '4010'
            elif sectcd[:1] == '5':
                sectcd = '5999'
            elif sectcd[:2] == '61':
                sectcd = '6120'
            elif sectcd[:2] == '62':
                sectcd = '6130'
            elif sectcd[:2] == '63':
                sectcd = '6310'
            elif sectcd[:2] in ('64','65','66','67','68','69'):
                sectcd = '6130'
            elif sectcd[:1] == '7':
                sectcd = '7199'
            elif sectcd[:2] in ('81','82'):
                sectcd = '8110'
            elif sectcd[:2] in ('83','84','85','86','87','88','89'):
                sectcd = '8999'
            elif sectcd[:2] == '91':
                sectcd = '9101'
            elif sectcd[:2] == '92':
                sectcd = '9410'
            elif sectcd[:2] in ('93','94','95'):
                sectcd = '9499'
            elif sectcd[:2] in ('96','97','98','99'):
                sectcd = '9999'

        records.append({
            "SECTORCD": sectorcd,
            "SECTCD":   sectcd,
            "STATECD":  statecd,
            "AMTIND":   amtind,
            "AMOUNT":   amount,
        })
    return pl.DataFrame(records)


alq = apply_sector_mapping(alq)

# ── DATA ALQ2: sector sub-grouping rollups ──
def build_alq2(alq: pl.DataFrame) -> pl.DataFrame:
    """
    Equivalent to DATA ALQ2 step: produce additional rows for sector hierarchies.
    Each WHEN block outputs rows with a parent SECTORCD for rollup purposes.
    """
    records = []

    def emit(row, sectorcd_override):
        r = {
            "SECTORCD": sectorcd_override,
            "SECTCD":   row["SECTCD"],
            "STATECD":  row["STATECD"],
            "AMTIND":   row["AMTIND"],
            "AMOUNT":   row["AMOUNT"],
        }
        records.append(r)

    for row in alq.iter_rows(named=True):
        sc = row["SECTCD"]

        # 1100 group
        if sc in ('1111','1112','1113','1114','1115','1116','1117','1119','1120','1130','1140','1150'):
            if sc in ('1111','1113','1115','1117','1119'):
                emit(row, '1110')
            emit(row, '1100')

        # 2200 group
        if sc in ('2210','2220'):
            emit(row, '2200')

        # 2300 group
        if sc in ('2301','2302','2303'):
            if sc in ('2301','2302'):
                emit(row, '2300')
            emit(row, '2300')
            if sc == '2303':
                emit(row, '2302')

        # 3100 group
        if sc in ('3110','3115','3111','3112','3113','3114'):
            if sc in ('3110','3113','3114'):
                emit(row, '3100')
            if sc in ('3115','3111','3112'):
                emit(row, '3110')

        if sc in ('3211','3212','3219'):
            emit(row, '3210')
        if sc in ('3221','3222'):
            emit(row, '3220')
        if sc in ('3231','3232'):
            emit(row, '3230')
        if sc in ('3241','3242'):
            emit(row, '3240')

        if sc in ('3270','3280','3290','3271','3272','3273','3311','3312','3313'):
            if sc in ('3270','3280','3290','3271','3272','3273'):
                emit(row, '3260')
            if sc in ('3271','3272','3273'):
                emit(row, '3270')
            if sc in ('3311','3312','3313'):
                emit(row, '3310')

        if sc in ('3431','3432','3433'):
            emit(row, '3430')
        if sc in ('3551','3552'):
            emit(row, '3550')
        if sc in ('3611','3619'):
            emit(row, '3610')

        if sc in ('3710','3720','3730','3720','3721','3731','3732'):
            emit(row, '3700')
            if sc == '3721':
                emit(row, '3720')
            if sc in ('3731','3732'):
                emit(row, '3730')

        if sc in ('3811','3812'):
            emit(row, '3800')
        if sc in ('3813','3814','3819'):
            emit(row, '3812')

        if sc in ('3832','3834','3835','3833'):
            emit(row, '3831')
            if sc == '3833':
                emit(row, '3832')

        if sc in ('3842','3843','3844'):
            emit(row, '3841')
        if sc in ('3851','3852','3853'):
            emit(row, '3850')
        if sc in ('3861','3862','3863','3864','3865','3866'):
            emit(row, '3860')
        if sc in ('3871','3872','3872'):
            emit(row, '3870')
        if sc in ('3891','3892','3893','3894'):
            emit(row, '3890')
        if sc in ('3911','3919'):
            emit(row, '3910')

        if sc in ('3951','3952','3953','3954','3955','3956','3957'):
            emit(row, '3950')
            if sc in ('3952','3953'):
                emit(row, '3951')
            if sc in ('3955','3956','3957'):
                emit(row, '3954')

        if sc in ('5001','5002','5003','5004','5005','5006','5008'):
            emit(row, '5010')
        if sc in ('6110','6120','6130'):
            emit(row, '6100')
        if sc in ('6310','6320'):
            emit(row, '6300')

        if sc in ('7111','7112','7117','7113','7114','7115','7116'):
            emit(row, '7110')
        if sc in ('7113','7114','7115','7116'):
            emit(row, '7112')
        if sc in ('7112','7114'):
            emit(row, '7113')
        if sc == '7116':
            emit(row, '7115')

        if sc in ('7121','7122','7123','7124'):
            emit(row, '7120')
            if sc == '7124':
                emit(row, '7123')
            if sc == '7122':
                emit(row, '7121')

        if sc in ('7131','7132','7133','7134'):
            emit(row, '7130')
        if sc in ('7191','7192','7193','7199'):
            emit(row, '7190')
        if sc in ('7210','7220'):
            emit(row, '7200')
        if sc in ('8110','8120','8130'):
            emit(row, '8100')

        if sc in ('8310','8330','8340','8320','8331','8332'):
            emit(row, '8300')
            if sc in ('8320','8331','8332'):
                emit(row, '8330')
        if sc == '8321':
            emit(row, '8320')
        if sc == '8333':
            emit(row, '8332')

        if sc in ('8420','8411','8412','8413','8414','8415','8416'):
            emit(row, '8400')
            if sc in ('8411','8412','8413','8414','8415','8416'):
                emit(row, '8410')

        if sc[:2] == '89':
            emit(row, '8900')
            if sc in ('8910','8911','8912','8913','8914'):
                if sc in ('8911','8912','8913','8914'):
                    emit(row, '8910')
                if sc == '8910':
                    emit(row, '8914')
            if sc in ('8921','8922','8920'):
                if sc in ('8921','8922'):
                    emit(row, '8920')
                if sc == '8920':
                    emit(row, '8922')
            if sc in ('8931','8932'):
                emit(row, '8930')
            if sc in ('8991','8999'):
                emit(row, '8990')

        if sc in ('9101','9102','9103'):
            emit(row, '9100')
        if sc in ('9201','9202','9203'):
            emit(row, '9200')
        if sc in ('9311','9312','9313','9314'):
            emit(row, '9300')

        if sc[:2] == '94':
            emit(row, '9400')
            if sc in ('9433','9434','9435','9432','9431','9440','9450'):
                if sc in ('9433','9434','9435'):
                    emit(row, '9432')
                emit(row, '9430')
            if sc in ('9410','9420'):
                emit(row, '9499')

    return pl.DataFrame(records) if records else pl.DataFrame(schema=alq.schema)


alq2 = build_alq2(alq)

# DATA ALQ: SET ALQ(IN=A) ALQ2; IF A THEN SECTORCD = SECTCD
# Rows from original ALQ get SECTORCD = SECTCD; rows from ALQ2 keep SECTORCD as-is
alq_from_a = alq.with_columns(pl.col("SECTCD").alias("SECTORCD"))
alq_merged = pl.concat([alq_from_a, alq2], how="diagonal")

# DATA ALQA: top-level rollup groups (1000, 2000, ... 9000)
def build_alqa(alq: pl.DataFrame) -> pl.DataFrame:
    records = []

    def emit(row, sectorcd_override):
        records.append({
            "SECTORCD": sectorcd_override,
            "SECTCD":   row.get("SECTCD", ""),
            "STATECD":  row["STATECD"],
            "AMTIND":   row["AMTIND"],
            "AMOUNT":   row["AMOUNT"],
        })

    group_map = {
        '1000': ('1100','1200','1300','1400'),
        '2000': ('2100','2200','2300','2400','2900'),
        '3000': ('3100','3120','3210','3220','3230','3240','3250','3260',
                 '3310','3430','3550','3610','3700','3800','3825','3831',
                 '3841','3850','3860','3870','3890','3910','3950','3960'),
        '4000': ('4010','4020','4030'),
        '5000': ('5010','5020','5030','5040','5050','5999'),
        '6000': ('6100','6300'),
        '7000': ('7110','7120','7130','7190','7200'),
        '8000': ('8100','8300','8400','8900'),
        '9000': ('9100','9200','9300','9400','9500','9600'),
    }

    for row in alq.iter_rows(named=True):
        sc = row["SECTORCD"]
        for parent, members in group_map.items():
            if sc in members:
                emit(row, parent)
                break  # only one group per row (ELSE IF chain in SAS)

    return pl.DataFrame(records) if records else pl.DataFrame(schema=alq.schema)


alqa = build_alqa(alq_merged)

# DATA ALQ: SET ALQ ALQA; IF SECTORCD EQ '    ' THEN SECTORCD = '9999'
alq_final = pl.concat([alq_merged, alqa], how="diagonal")
alq_final = alq_final.with_columns(
    pl.when(pl.col("SECTORCD").str.strip_chars() == '')
    .then(pl.lit('9999'))
    .otherwise(pl.col("SECTORCD"))
    .alias("SECTORCD")
)

# /*** NEW SECTOR MAPPING END ***/

# DATA ALQLOAN: KEEP=BNMCODE AMTIND AMOUNT
# IF SECTORCD NE '9700' THEN BNMCODE='340000000'||SECTORCD||STATECD
alqloan2_records = []
for row in alq_final.iter_rows(named=True):
    sectorcd = str(row.get("SECTORCD", "") or "")
    statecd  = str(row.get("STATECD",  "") or "")
    amtind   = str(row.get("AMTIND",   "") or "")
    amount   = row.get("AMOUNT", 0) or 0

    if sectorcd != '9700':
        bnmcode = ('340000000' + sectorcd + statecd)[:14]
        alqloan2_records.append({"BNMCODE": bnmcode, "AMTIND": amtind, "AMOUNT": amount})

alqloan2 = pl.DataFrame(
    alqloan2_records,
    schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64},
)

# PROC APPEND DATA=ALQLOAN BASE=BNM.LALQ{REPTMON}{NOWK}
append_to_lalq(alqloan2)
# PROC DATASETS: DELETE ALQ ALQLOAN (handled by scope)


# ─────────────────────────────────────────────────────────────
# SECTION 3: FIXED AND FLOATING RATE LOANS
# OPTION MISSING IS TO INCLUDE THOSE ODS WITH USURYIDX = .
# WHERE PRODCD starts with '34' OR '54'
# ─────────────────────────────────────────────────────────────
con = duckdb.connect()
con.register("loan_tbl", df_loan_raw)
alq_rate = con.execute("""
    SELECT PRODCD, PRODUCT, USURYIDX, AMTIND,
           SUM(BAL_AFT_EIR) AS AMOUNT
    FROM loan_tbl
    WHERE SUBSTR(PRODCD, 1, 2) = '34'
       OR SUBSTR(PRODCD, 1, 2) = '54'
    GROUP BY PRODCD, PRODUCT, USURYIDX, AMTIND
""").pl()
con.close()

# DATA ALQLOAN: IF PRODCD IN ('34180','34380') THEN BIC=PUT(PRODUCT,ODRATE.)
#               ELSE IF PRODCD='34240'         THEN BIC=PUT(PRODUCT,ODRATE.)
#               ELSE BIC=PUT(PRODUCT,LNRATE.)
alqloan_rate_records = []
for row in alq_rate.iter_rows(named=True):
    prodcd  = str(row.get("PRODCD",  "") or "")
    product = row.get("PRODUCT")
    amtind  = str(row.get("AMTIND",  "") or "")
    amount  = row.get("AMOUNT", 0) or 0

    if prodcd in ('34180', '34380'):
        bic = odrate_format(product)
    elif prodcd == '34240':
        bic = odrate_format(product)
    else:
        bic = lnrate_format(product)

    if bic.strip() == '':
        continue

    bnmcode = (bic + '00000000Y')[:14]
    alqloan_rate_records.append({"BNMCODE": bnmcode, "AMTIND": amtind, "AMOUNT": amount})

alqloan_rate = pl.DataFrame(
    alqloan_rate_records,
    schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64},
)

# PROC SUMMARY DATA=ALQLOAN NWAY: CLASS BNMCODE AMTIND; SUM AMOUNT
if not alqloan_rate.is_empty():
    alqloan_rate = (
        alqloan_rate.group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
    )

# * PROC APPEND DATA=ALQLOAN BASE=BNM.LALQ&REPTMON&NOWK; RUN;
# (commented out in original SAS - not appended)
# PROC DATASETS: DELETE ALQ UALQ ALQLOAN (handled by scope)


# ─────────────────────────────────────────────────────────────
# FINAL CONSOLIDATION
# PROC SUMMARY DATA=BNM.LALQ{REPTMON}{NOWK} NWAY
# CLASS BNMCODE AMTIND; VAR AMOUNT; SUM=AMOUNT
# ─────────────────────────────────────────────────────────────
if os.path.exists(LALQ_PARQUET):
    con = duckdb.connect()
    lalq_all = con.execute(f"SELECT * FROM read_parquet('{LALQ_PARQUET}')").pl()
    con.close()

    lalq_final = (
        lalq_all.group_by(["BNMCODE", "AMTIND"])
        .agg(pl.col("AMOUNT").sum())
        .sort(["BNMCODE", "AMTIND"])
    )
    lalq_final.write_parquet(LALQ_PARQUET)
else:
    lalq_final = pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMTIND": pl.Utf8, "AMOUNT": pl.Float64})


# ─────────────────────────────────────────────────────────────
# PROC PRINT OUTPUT
# OPTIONS NOCENTER NODATE NONUMBER
# TITLE1 &SDESC
# TITLE2 'REPORT ON DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN'
# TITLE3 'REPORT DATE : ' &RDATE
# FORMAT AMOUNT COMMA25.2
# ─────────────────────────────────────────────────────────────
def format_report(df: pl.DataFrame) -> str:
    """
    Generate PROC PRINT style report with ASA carriage control characters.
    FORMAT AMOUNT COMMA25.2 => right-justified, comma-separated, 2 decimal places
    """
    lines = []
    # ASA '1' = new page / first page
    lines.append(f"1{SDESC}")
    lines.append(f" REPORT ON DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN")
    lines.append(f" REPORT DATE : {RDATE}")
    lines.append(f" ")

    col_obs    = "OBS"
    col_bnm    = "BNMCODE"
    col_amt    = "AMTIND"
    col_amount = "AMOUNT"

    hdr = f" {col_obs:<6} {col_bnm:<14}  {col_amt:<6}  {col_amount:>25}"
    lines.append(f" {hdr}")
    lines.append(f" {'-' * len(hdr)}")

    obs = 0
    page_body_lines = 0
    first_page = True

    for row in df.iter_rows(named=True):
        # Paginate: header takes ~5 lines; after PAGE_LENGTH - 5 body lines, new page
        if not first_page and page_body_lines >= PAGE_LENGTH - 5:
            lines.append(f"1{SDESC}")
            lines.append(f" REPORT ON DOMESTIC ASSETS AND LIABILITIES PART III - M&I LOAN")
            lines.append(f" REPORT DATE : {RDATE}")
            lines.append(f" ")
            lines.append(f" {hdr}")
            lines.append(f" {'-' * len(hdr)}")
            page_body_lines = 0

        obs += 1
        amount_fmt = f"{row['AMOUNT']:>25,.2f}"
        data_line = f"  {obs:<6} {row['BNMCODE']:<14}  {row['AMTIND']:<6}  {amount_fmt}"
        lines.append(data_line)
        page_body_lines += 1
        first_page = False

    return "\n".join(lines)


report_str = format_report(lalq_final)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(report_str)
    f.write("\n")

print(f"Output report written to : {OUTPUT_FILE}")
print(f"Final LALQ dataset       : {LALQ_PARQUET}")
