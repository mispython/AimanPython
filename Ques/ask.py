Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMECPT_refine.py", line 147, in <module>
    def _decode_ebcdic_int(raw: bytes) -> int | None:
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ 

============================================
def _decode_ebcdic_int(raw: bytes) -> int | None:
    """Decode an EBCDIC numeric field, returning None when it is not numeric."""
    decoded = raw.decode(STR_ENCODING, errors="replace").strip()
    return int(decoded) if decoded.isdigit() else None


def _is_valid_packed_decimal(raw: bytes) -> bool:
    """Return True when raw bytes look like a valid packed-decimal field."""
    if not raw:
        return False

    for byte in raw[:-1]:
        if (byte >> 4) > 9 or (byte & 0x0F) > 9:
            return False

    last_byte = raw[-1]
    return (last_byte >> 4) <= 9 and (last_byte & 0x0F) in (0x0C, 0x0D, 0x0F)



def _score_lrecl_candidate(
    filepath: Path,
    content_len: int,
    lrecl: int,
    sample_size: int = 25,
) -> tuple[int, int]:
    """
    Score a candidate physical record length using fields that must be valid
    when record boundaries are correct.  Returns (score, sampled_records).



    records_to_sample = min(sample_size, file_size // lrecl)
    if records_to_sample == 0:
        return (0, 0)

    score = 0


  for _ in range(records_to_sample):
            raw = fh.read(lrecl)[:content_len]
            if len(raw) < content_len:
                break

            acctno = _decode_ebcdic_int(raw[0:11])
            tranyy = _decode_ebcdic_int(raw[91:95])
            tranmm = _decode_ebcdic_int(raw[96:98])
            trandd = _decode_ebcdic_int(raw[99:101])

            if acctno and acctno > 0:
                score += 2
            if tranyy and 1950 <= tranyy <= 2099:
                score += 1
            if tranmm and 1 <= tranmm <= 12:
                score += 1
            if trandd and 1 <= trandd <= 31:
                score += 1
            if tranyy and tranmm and trandd:
                try:
                    date(tranyy, tranmm, trandd)
                    score += 2
                except ValueError:
                    pass
            if _is_valid_packed_decimal(raw[109:118]):
                score += 2

    return (score, records_to_sample)



@@ -22,60 +22,61 @@ import duckdb

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")

# Input flat file (mainframe fixed-width, RECFM=FB LRECL=1150)
INPUT_DIR = BASE_DIR / "input/uat" / "DP_PBECP_20260511"

# Output files (equivalent of SAP.PBB.ECPTRN.ETRNFTP / ECISFTP)
OUTPUT_DIR   = BASE_DIR / "output" / "EIBMECPT"
ETRNFTP_FILE = OUTPUT_DIR / "ETRNFTP.txt"
ECISFTP_FILE = OUTPUT_DIR / "ECISFTP.txt"

# Weekly ECP Parquet store (equivalent of ECPOUT library)
ECPOUT_DIR = OUTPUT_DIR / "ECPOUT"

# ---------------------------------------------------------------------------
# FILE ENCODING NOTE
# ---------------------------------------------------------------------------
# The input file is pure EBCDIC IBM Code Page 037 (cp037).  WinSCP displays
# it as "Encoding: 1252 (ANSI)" because it renders the raw EBCDIC byte values
# through the Windows-1252 display font -- this is a display artefact only
# and does NOT mean the file was converted to latin-1.
#
# Record structure: RECFM=FB, LRECL=1150.
# Records are separated by EBCDIC newline byte 0x25 (one byte per record),
# which is the EBCDIC equivalent of a line-feed.  When read as raw binary
# the separator byte value is 0x25, NOT the ASCII 0x0A.
# Record structure: RECFM=FB, LRECL=1150 content bytes.
# Depending on the transfer path, each record may also be followed by one or
# more separator/control bytes.  For example, EBCDIC newline is raw byte 0x25,
# which is the EBCDIC equivalent of a line-feed, NOT ASCII 0x0A.
#
# The AMOUNT field (PD9.2, packed-decimal BCD, 9 bytes at offset @110) is
# pure binary and must be decoded with the packed-decimal algorithm, not
# as an EBCDIC character string.
# ---------------------------------------------------------------------------
LRECL_CONTENT  = 1150        # bytes of actual record content (no separator)
MAX_TRAILING_BYTES = 8       # scan up to this many non-content bytes per record
STR_ENCODING   = "cp037"     # all string/numeric fields: EBCDIC IBM Code Page 037

# Ensure directories exist
ECPOUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
@@ -121,100 +122,188 @@ def decode_packed_decimal(raw: bytes, decimal_places: int = 2) -> float:
    This field is raw binary and is not subject to EBCDIC character encoding.
    """
    if not raw:
        return 0.0

    sign_nibble = 0xF   # default: unsigned positive
    digits = ""
    for i, byte in enumerate(raw):
        high = (byte >> 4) & 0x0F
        low  = byte & 0x0F
        if i < len(raw) - 1:
            digits += str(high) + str(low)
        else:
            # Last byte: high nibble = last digit, low nibble = sign
            digits += str(high)
            sign_nibble = low

    value = int(digits) / (10 ** decimal_places) if digits else 0.0
    if sign_nibble == 0xD:      # negative
        value = -value
    return value

# =============================================================================
# RECORD-LENGTH AUTO-DETECTION
# =============================================================================
def _detect_lrecl(filepath: Path, content_len: int) -> int:
    """
    Determine the effective per-record read size by detecting whether an
    EBCDIC newline byte (0x25) is appended after each record.
def _decode_ebcdic_int(raw: bytes) -> int | None:
    """Decode an EBCDIC numeric field, returning None when it is not numeric."""
    decoded = raw.decode(STR_ENCODING, errors="replace").strip()
    return int(decoded) if decoded.isdigit() else None


def _is_valid_packed_decimal(raw: bytes) -> bool:
    """Return True when raw bytes look like a valid packed-decimal field."""
    if not raw:
        return False

    for byte in raw[:-1]:
        if (byte >> 4) > 9 or (byte & 0x0F) > 9:
            return False

    last_byte = raw[-1]
    return (last_byte >> 4) <= 9 and (last_byte & 0x0F) in (0x0C, 0x0D, 0x0F)

    The EBCDIC newline is raw byte 0x25 -- distinct from ASCII LF (0x0A).
    Peeks at the byte immediately following the first content_len bytes:
      - 0x25  =>  EBCDIC newline separator  =>  effective = content_len + 1
      - 0x0A  =>  ASCII LF separator        =>  effective = content_len + 1
      - 0x0D 0x0A  =>  CRLF separator       =>  effective = content_len + 2
      - other =>  no separator              =>  effective = content_len

    Validates against file_size % candidate == 0.
def _score_lrecl_candidate(
    filepath: Path,
    content_len: int,
    lrecl: int,
    sample_size: int = 25,
) -> tuple[int, int]:
    """
    Score a candidate physical record length using fields that must be valid
    when record boundaries are correct.  Returns (score, sampled_records).
    """
    file_size = filepath.stat().st_size
    records_to_sample = min(sample_size, file_size // lrecl)
    if records_to_sample == 0:
        return (0, 0)

    score = 0
    with open(filepath, "rb") as fh:
        fh.read(content_len)
        peek = fh.read(2)
        for _ in range(records_to_sample):
            raw = fh.read(lrecl)[:content_len]
            if len(raw) < content_len:
                break

            acctno = _decode_ebcdic_int(raw[0:11])
            tranyy = _decode_ebcdic_int(raw[91:95])
            tranmm = _decode_ebcdic_int(raw[96:98])
            trandd = _decode_ebcdic_int(raw[99:101])

            if acctno and acctno > 0:
                score += 2
            if tranyy and 1950 <= tranyy <= 2099:
                score += 1
            if tranmm and 1 <= tranmm <= 12:
                score += 1
            if trandd and 1 <= trandd <= 31:
                score += 1
            if tranyy and tranmm and trandd:
                try:
                    date(tranyy, tranmm, trandd)
                    score += 2
                except ValueError:
                    pass
            if _is_valid_packed_decimal(raw[109:118]):
                score += 2

    return (score, records_to_sample)


    if len(peek) >= 2 and peek[0] == 0x0D and peek[1] == 0x0A:
        candidate = content_len + 2     # CRLF
    elif len(peek) >= 1 and peek[0] in (0x25, 0x0A):
        candidate = content_len + 1     # EBCDIC newline or ASCII LF
def _format_candidate_list(candidates: list[int]) -> str:
    """Format candidate LRECLs for messages."""
    return ", ".join(str(candidate) for candidate in candidates)


def _detect_lrecl(filepath: Path, content_len: int) -> int:
    """
    Determine the physical bytes to read per record.

    The ECP content layout is fixed at 1150 bytes, but file transfers may add
    per-record separator/control bytes.  The daily file size changes with the
    number of records, so detection must use divisibility by the physical record
    stride, not yesterday's total file size.

    We scan content_len through content_len + MAX_TRAILING_BYTES.  This covers:
      - 1150     => pure fixed-block content
      - 1151     => one-byte separator such as EBCDIC NL (0x25) or LF (0x0A)
      - 1152     => two-byte separator such as CRLF
      - 1153+    => transferred files with extra per-record control bytes

    If no candidate divides the file exactly, stop instead of producing shifted
    rows.  A shifted fixed-width parse is worse than no output because columns
    such as ACCTNO, SERIAL, and TRANDATE silently become corrupted.
    """
    file_size = filepath.stat().st_size
    candidates = list(range(content_len, content_len + MAX_TRAILING_BYTES + 1))

    if file_size == 0:
        print(f"[EIBMECPT] Empty input file; using content LRECL={content_len}")
        return content_len

    exact_candidates = [
        candidate for candidate in candidates if file_size % candidate == 0
    ]
    if not exact_candidates:
        raise ValueError(
            f"[EIBMECPT] Cannot determine fixed record length: file size {file_size} "
            f"is not divisible by candidates ({_format_candidate_list(candidates)}). "
            "Do not continue with LRECL=1150 because output would be misaligned. "
            "Verify the source file transfer mode, trailing per-record bytes, or LRECL."
        )

    scored_candidates = []
    for candidate in exact_candidates:
        score, sampled = _score_lrecl_candidate(filepath, content_len, candidate)
        scored_candidates.append((score, sampled, candidate))

    score, sampled, detected_lrecl = max(scored_candidates)
    record_count = file_size // detected_lrecl

    with open(filepath, "rb") as fh:
        fh.seek(content_len)
        trailing_sample = fh.read(max(0, detected_lrecl - content_len))

    if detected_lrecl == content_len:
        separator_desc = "none (pure 1150-byte content)"
    elif trailing_sample in (b"\x25", b"\x0A"):
        separator_desc = f"single byte separator 0x{trailing_sample[0]:02X}"
    elif trailing_sample == b"\x0D\x0A":
        separator_desc = "CRLF separator 0x0D0A"



        separator_desc = (
            f"{detected_lrecl - content_len} trailing byte(s): "
            f"{trailing_sample.hex().upper()}"
        )

    print(
        f"[EIBMECPT] Detected effective LRECL={detected_lrecl} "
        f"(content={content_len}, {separator_desc}, records={record_count}, "
        f"validation_score={score}/{sampled * 9 if sampled else 0})"
    )
    return detected_lrecl

===============================

[EIBMECPT] WARNING: file size 18343077 not divisible by candidates (1150, 1151, 1152). Using LRECL=1150 -- output may be misaligned.

  /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMECPT_refine.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMECPT_refine.py
[EIBMECPT] reptdate=2026-05-12  REPTMON=05  NOWK=2  REPTYEAR=26
[EIBMECPT] Weekly store : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECPOUT/ECP052.txt
[EIBMECPT] TRN output   : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ETRNFTP.txt
[EIBMECPT] CIS output   : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECISFTP.txt
[EIBMECPT] Reading flat file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/DP_PBECP_20260512
[EIBMECPT] WARNING: file size 18343077 not divisible by candidates (1150, 1151, 1152). Using LRECL=1150 -- output may be misaligned.
[EIBMECPT] Records read from flat file: 15951
[EIBMECPT] Weekly store updated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECPOUT/ECP052.txt  (15951 rows)
[EIBMECPT] TRN file written : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ETRNFTP.txt  (15951 rows)
[EIBMECPT] CIS file written : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMECPT/ECISFTP.txt  (15951 rows)
[EIBMECPT] Program completed successfully.

 ========== PREVIEW ========== 

shape: (15_951, 12)
┌────────────┬──────────────────┬────────────┬─────────────┬───┬──────────────┬──────────────────┬────────┬─────────────────────────────────┐
│ ACCTNO     ┆ SERIAL           ┆ TRANDATE   ┆ BENEBANKBIC ┆ … ┆ PAYORCORPREF ┆ BENEREF          ┆ STATUS ┆ RSONDESC                        │
│ ---        ┆ ---              ┆ ---        ┆ ---         ┆   ┆ ---          ┆ ---              ┆ ---    ┆ ---                             │
│ i64        ┆ str              ┆ date       ┆ str         ┆   ┆ str          ┆ str              ┆ str    ┆ str                             │
╞════════════╪══════════════════╪════════════╪═════════════╪═══╪══════════════╪══════════════════╪════════╪═════════════════════════════════╡
│ 3077509028 ┆ 2026-05-12063123 ┆ 2026-05-12 ┆ ARBKMYKL    ┆ … ┆              ┆ 188579           ┆ SC     ┆                                 │
│ 3077509    ┆ 0282026-05-12063 ┆ null       ┆    MBBEMYKL ┆ … ┆              ┆    IKUL000595196 ┆        ┆ SC                             │
│ 3077       ┆ 5090282026-05-12 ┆ null       ┆       MBBEM ┆ … ┆              ┆       700256404  ┆        ┆ SC                             │
│ 3          ┆ 0775090282026-05 ┆ null       ┆          AR ┆ … ┆              ┆          188585  ┆        ┆    SC                          │
│ 0          ┆  030775090282026 ┆ null       ┆             ┆ … ┆ 
                                                                             ┆             1885 ┆        ┆       SC                       │
│ …          ┆ …                ┆ …          ┆ …           ┆ … ┆ …            ┆ …                ┆ …      ┆ …                               │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                           5148… │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                              5… │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                               … │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                               … │
│ 0          ┆                  ┆ null       ┆             ┆ … ┆              ┆                  ┆        ┆                                 │
└────────────┴──────────────────┴────────────┴─────────────┴───┴──────────────┴──────────────────┴────────┴─────────────────────────────────┘
shape: (15_951, 8)
┌────────────┬──────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────────┬───────────┬────────────┬──────────┐
│ ACCTNO     ┆ SERIAL           ┆ BENENAME                        ┆ BNAD                            ┆ BENEID             ┆ BENEIDIND ┆ MOBIPHON   ┆ EMAILADD │
│ ---        ┆ ---              ┆ ---                             ┆ ---                             ┆ ---                ┆ ---       ┆ ---        ┆ ---      │
│ i64        ┆ str              ┆ str                             ┆ str                             ┆ str                ┆ str       ┆ str        ┆ str      │
╞════════════╪══════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════════╪═══════════╪════════════╪══════════╡
│ 3077509028 ┆ 2026-05-12063123 ┆ THARMA NEWS AGENT               ┆  M-09, LORONG RASAK, TAMAN SET… ┆                    ┆           ┆            ┆          │
│ 3077509    ┆ 0282026-05-12063 ┆    CITY-LINK EXPRESS (M) SDN B… ┆ MBB WISMA CITY-LINK, NO:3A, JL… ┆                    ┆           ┆ 1          ┆          │
│ 3077       ┆ 5090282026-05-12 ┆       BURSA MALAYSIA DEPOSITOR… ┆    MBB FINANCE DEPARTMENT     … ┆                    ┆           ┆ 1-01       ┆          │
│ 3          ┆ 0775090282026-05 ┆          THARMA NEWS AGENT      ┆       AMB M-09, LORONG RASAK, … ┆                    ┆           ┆ 1-01-01    ┆          │
│ 0          ┆  030775090282026 ┆ 83          THARMA NEWS AGENT   ┆          AMB M-09, LORONG RASA… ┆                    ┆           ┆ 0001-01-01 ┆          │
│ …          ┆ …                ┆ …                               ┆ …                               ┆ …                  ┆ …         ┆ …          ┆ …        │
│ 0          ┆                  ┆                               … ┆ -01                           … ┆       GKLC05120083 ┆ 28        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆ -01-01                        … ┆          GKLC05120 ┆ 08        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆ 001-01-01                     … ┆             GKLC05 ┆ 12        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆   0001-01-01                  … ┆                GKL ┆ C0        ┆            ┆          │
│ 0          ┆                  ┆                               … ┆      0001-01-01               … ┆                    ┆ GK        ┆            ┆          │
└────────────┴──────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────────┴───────────┴────────────┴──────────┘
===============================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py", line 215, in <module>
    ctcs_out.write_parquet(str(CTCS_MON_PQ))
NameError: name 'CTCS_MON_PQ' is not defined
================================
def _resolve_reptdate(
    source_date: Optional[date] = None,
    today: Optional[date] = None,
) -> date:
    """Return the source/header report date, or yesterday if none is supplied."""
    if source_date is not None:
        return source_date

    run_date = today or datetime.now().date()
    return run_date - timedelta(days=1)

_reptdate_val: date = _resolve_reptdate(source_date=_tdate)

REPTDAY   = f"{_reptdate_val.day:02d}"
REPTMON   = f"{_reptdate_val.month:02d}"
REPTYEAR  = str(_reptdate_val.year)[-2:]
# &RDATE   = PUT(REPTDATE, Z5.) -- zero-padded 5-digit SAS date number
# Reproduced as DDMMYY8. string for comparison (used as &REPTDATE too)
RDATE_STR = _reptdate_val.strftime("%d/%m/%y")    # DDMMYY8. format

# Monthly accumulator path
CTCS_MON_PQ = os.path.join(OUTPUT_DIR, f"CTCS{REPTMON}.parquet")

source /sas/python/virt_edw_dev/bin/activate
/sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
THE SAP.PBB.EPCU.CTCS IS NOT DATED 11/05/26
================================
source /sas/python/virt_edw_dev/bin/activate
/sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
[sas_edw_dev@svdwh004 Data_Warehouse]$ source /sas/python/virt_edw_dev/bin/activate
(virt_edw_dev) [sas_edw_dev@svdwh004 Data_Warehouse]$ /sas/python/virt_edw_dev/bin/python3 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCTCS.py", line 88, in <module>
    def _resolve_reptdate(today: date | None = None) -> date:
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'

def _resolve_reptdate(today: date | None = None) -> date:
    """Return the report date for data generated today but dated yesterday."""
    run_date = today or datetime.now().date()
    return run_date - timedelta(days=1)
==================================
