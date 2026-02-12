#!/usr/bin/env python3
"""
Program: PBBALF

1) DATA AL step
   - INPUT BNMCODE $ 1-14 DESC $ 25-68
   - IF SUBSTR(BNMCODE, 1, 1) IN ('3','4')
2) PROC FORMAT
   - VALUE $BRCH
   - INVALUE BRANCH

Implementation notes:
- Prefer DuckDB to read parquet input for DATA AL when parquet exists.
- Fallback to parsing inline CARDS from the SAS source when parquet is absent.
- Output AL as fixed-width .txt with SAS column positions.
- Export parsed format definitions to .txt files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import importlib.util
import re
from typing import Iterable, List, Tuple


# =============================================================================
# PATH CONFIGURATION
# =============================================================================

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SAS_SOURCE_PATH = REPO_ROOT / "Ori_SAS" / "PBBALF"
AL_INPUT_PARQUET_PATH = INPUT_DIR / "PBBALF_input.parquet"

AL_OUTPUT_TXT_PATH = OUTPUT_DIR / "AL.txt"
BRCH_OUTPUT_TXT_PATH = OUTPUT_DIR / "BRCH_FORMAT.txt"
BRANCH_INVALUE_OUTPUT_TXT_PATH = OUTPUT_DIR / "BRANCH_INVALUE.txt"


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True)
class ALRecord:
    bnmcode: str
    desc: str


@dataclass(frozen=True)
class BranchName:
    code: str
    name: str


@dataclass(frozen=True)
class BranchInvalueRule:
    exact_values: Tuple[int, ...]
    ranges: Tuple[Tuple[int, int], ...]

    def apply(self, value: int) -> int | None:
        if value in self.exact_values:
            return 1
        for start, end in self.ranges:
            if start <= value <= end:
                return 1
        return None


# =============================================================================
# SOURCE PARSING
# =============================================================================


def read_sas_source() -> str:
    if not SAS_SOURCE_PATH.exists():
        raise FileNotFoundError(f"SAS source not found: {SAS_SOURCE_PATH}")
    return SAS_SOURCE_PATH.read_text(encoding="utf-8")


def extract_cards_block(sas_text: str) -> str:
    match = re.search(r"CARDS;\n(.*?)\n;\nRUN;", sas_text, flags=re.DOTALL)
    if not match:
        raise ValueError("Could not locate CARDS block in Ori_SAS/PBBALF")
    return match.group(1)


def parse_al_from_cards(sas_text: str) -> List[ALRecord]:
    records: List[ALRecord] = []
    for line in extract_cards_block(sas_text).splitlines():
        if not line.strip():
            continue

        # SAS: BNMCODE $ 1-14, DESC $ 25-68
        bnmcode = line[0:14].strip()
        desc = line[24:68].strip() if len(line) >= 25 else ""

        if not bnmcode:
            continue
        if bnmcode[0] not in {"3", "4"}:
            continue

        records.append(ALRecord(bnmcode=bnmcode, desc=desc))

    return records


def parse_brch_value_format(sas_text: str) -> List[BranchName]:
    block_match = re.search(
        r"PROC FORMAT;\n\s*VALUE \$BRCH\n(.*?)\n\s*INVALUE BRANCH",
        sas_text,
        flags=re.DOTALL,
    )
    if not block_match:
        raise ValueError("Could not locate VALUE $BRCH block in Ori_SAS/PBBALF")

    entries: List[BranchName] = []
    for raw_line in block_match.group(1).splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = re.match(r"'([^']+)'\s*=\s*'([^']*)';?", line)
        if match:
            entries.append(BranchName(code=match.group(1), name=match.group(2)))
            continue

        other_match = re.match(r"OTHER\s*=\s*'([^']*)';?", line)
        if other_match:
            entries.append(BranchName(code="OTHER", name=other_match.group(1)))

    return entries


def parse_branch_invalue(sas_text: str) -> BranchInvalueRule:
    match = re.search(r"INVALUE BRANCH\n(.*?)\s*=\s*1;", sas_text, flags=re.DOTALL)
    if not match:
        raise ValueError("Could not locate INVALUE BRANCH block in Ori_SAS/PBBALF")

    token_text = match.group(1).replace("\n", " ")
    tokens = [token.strip() for token in token_text.split(",") if token.strip()]

    exact_values: List[int] = []
    ranges: List[Tuple[int, int]] = []

    for token in tokens:
        if "-" in token:
            start_str, end_str = [part.strip() for part in token.split("-", 1)]
            ranges.append((int(start_str), int(end_str)))
        else:
            exact_values.append(int(token))

    return BranchInvalueRule(exact_values=tuple(exact_values), ranges=tuple(ranges))


# =============================================================================
# PARQUET READER (DuckDB preferred)
# =============================================================================


def _duckdb_available() -> bool:
    return importlib.util.find_spec("duckdb") is not None


def _pyarrow_available() -> bool:
    return importlib.util.find_spec("pyarrow") is not None


def read_al_from_parquet(parquet_path: Path) -> List[ALRecord]:
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing parquet input: {parquet_path}")

    if _duckdb_available():
        import duckdb  # type: ignore

        query = """
            SELECT
                CAST(BNMCODE AS VARCHAR) AS BNMCODE,
                CAST(DESC AS VARCHAR) AS DESC
            FROM parquet_scan(?)
            WHERE SUBSTR(CAST(BNMCODE AS VARCHAR), 1, 1) IN ('3', '4')
        """
        rows = duckdb.execute(query, [str(parquet_path)]).fetchall()
        return [ALRecord(bnmcode=str(row[0]).strip(), desc=str(row[1]).strip()) for row in rows]

    if _pyarrow_available():
        import pyarrow.parquet as pq  # type: ignore

        table = pq.read_table(parquet_path, columns=["BNMCODE", "DESC"])
        bnmcode_col = table.column("BNMCODE")
        desc_col = table.column("DESC")

        records: List[ALRecord] = []
        for i in range(table.num_rows):
            bnmcode = str(bnmcode_col[i].as_py() or "").strip()
            desc = str(desc_col[i].as_py() or "").strip()
            if bnmcode and bnmcode[0] in {"3", "4"}:
                records.append(ALRecord(bnmcode=bnmcode, desc=desc))
        return records

    raise ModuleNotFoundError(
        "Neither duckdb nor pyarrow is available to read parquet input. "
        "Install duckdb (preferred) or pyarrow."
    )


# =============================================================================
# FORMAT APPLICATION HELPERS
# =============================================================================


def format_brch(branch_code: str, mappings: Iterable[BranchName]) -> str:
    default_value = "OTHER"
    for entry in mappings:
        if entry.code == "OTHER":
            default_value = entry.name
            break

    for entry in mappings:
        if entry.code == branch_code:
            return entry.name

    return default_value


# =============================================================================
# WRITERS
# =============================================================================


def write_al_fixed_width_txt(records: List[ALRecord], output_path: Path) -> None:
    # SAS layout: BNMCODE at col 1-14, DESC at col 25-68.
    lines: List[str] = []
    for row in records:
        bnmcode = row.bnmcode[:14]
        desc = row.desc[:44]
        lines.append(f"{bnmcode:<14}{'':10}{desc:<44}\n")
    output_path.write_text("".join(lines), encoding="utf-8")


def write_brch_format_txt(entries: List[BranchName], output_path: Path) -> None:
    # Preserve original SAS order.
    lines = [f"{entry.code}={entry.name}\n" for entry in entries]
    output_path.write_text("".join(lines), encoding="utf-8")


def write_branch_invalue_txt(rule: BranchInvalueRule, output_path: Path) -> None:
    exact_line = ",".join(str(v) for v in rule.exact_values)
    range_line = ",".join(f"{start}-{end}" for start, end in rule.ranges)
    output_path.write_text(
        f"EXACT:{exact_line}\nRANGES:{range_line}\nASSIGNMENT:1\n",
        encoding="utf-8",
    )


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    sas_text = read_sas_source()

    if AL_INPUT_PARQUET_PATH.exists():
        al_records = read_al_from_parquet(AL_INPUT_PARQUET_PATH)
        source_used = f"parquet: {AL_INPUT_PARQUET_PATH}"
    else:
        al_records = parse_al_from_cards(sas_text)
        source_used = "SAS inline CARDS"

    brch_entries = parse_brch_value_format(sas_text)
    branch_rule = parse_branch_invalue(sas_text)

    write_al_fixed_width_txt(al_records, AL_OUTPUT_TXT_PATH)
    write_brch_format_txt(brch_entries, BRCH_OUTPUT_TXT_PATH)
    write_branch_invalue_txt(branch_rule, BRANCH_INVALUE_OUTPUT_TXT_PATH)

    print(f"Source used for AL: {source_used}")
    print(f"AL rows written: {len(al_records)} -> {AL_OUTPUT_TXT_PATH}")
    print(f"$BRCH entries written: {len(brch_entries)} -> {BRCH_OUTPUT_TXT_PATH}")
    print(f"INVALUE BRANCH exact values: {len(branch_rule.exact_values)}")
    print(f"INVALUE BRANCH ranges: {len(branch_rule.ranges)} -> {BRANCH_INVALUE_OUTPUT_TXT_PATH}")


if __name__ == "__main__":
    main()
