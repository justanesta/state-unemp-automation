"""Step 1 – Ingest, validate, and gate-check the raw unemployment xlsx.

Standalone: python validate.py [--input raw_data/*.xlsx]
Module:     from validate import run_validation
"""

from __future__ import annotations

import glob
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

import states as states_module

# ---------------------------------------------------------------------------
# Configuration constants (adjust as needed)
# ---------------------------------------------------------------------------

IMPLAUSIBLE_RATE_LOWER_BOUND: float = 0.0
IMPLAUSIBLE_RATE_UPPER_BOUND: float = 100.0
RATE_WARNING_THRESHOLD: float = 15.0       # non-blocking warning flag
PUBLISH_GATE_THRESHOLD: float = 0.40       # abort if this fraction of states are fully unpublishable
TOTAL_STATES: int = 50

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class RawRow(BaseModel):
    """Structural validation only — one row straight off the xlsx."""
    model_config = ConfigDict(strict=False)

    state: str
    state_code: str
    month: str
    unemployment_rate: float | None = None
    unemployment_rate_prev_month: float | None = None
    source: str


class ValidatedRow(BaseModel):
    """Output of the validation step — business-logic checked and normalized."""
    state_canonical: str
    state_code: str
    month_canonical: str                          # "YYYY-MM"
    unemployment_rate: float | None
    unemployment_rate_prev_month: float | None
    source: str
    source_row_index: int                         # 1-based row number in xlsx
    qa_flags: list[str]
    is_publishable: bool


class CleanRow(BaseModel):
    """Schema for each line in the clean_data JSONL (pivoted long format)."""
    state_canonical: str
    state_code: str
    date: str                                     # "YYYY-MM-DD", day always 01
    value: float
    source: str
    ingest_run: str                               # ISO 8601 pipeline run timestamp
    source_row_index: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

_DATE_PATTERN = re.compile(r"^(\d{4})[/-](\d{2})$")


def _normalize_date(raw: str) -> str | None:
    """Parse 'YYYY-MM' or 'YYYY/MM' → 'YYYY-MM'.  Returns None if unparseable."""
    m = _DATE_PATTERN.match(raw.strip())
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def _read_xlsx(filepath: str) -> list[dict]:
    """Read the xlsx and return rows as dicts keyed by header name.

    The dataset sheet is named 'in'.
    """
    import openpyxl

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb["in"]

    rows_iter = ws.iter_rows(values_only=True)
    header = [str(cell).strip() if cell else "" for cell in next(rows_iter)]

    records: list[dict] = []
    for row in rows_iter:
        record = {}
        for col_name, val in zip(header, row):
            # openpyxl may return floats for numeric cells; keep as-is
            record[col_name] = val
        records.append(record)

    wb.close()
    return records


def _find_input_file() -> str:
    """Glob raw_data/ for exactly one xlsx.  Raises if 0 or >1 found."""
    matches = glob.glob("raw_data/*.xlsx")
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected exactly 1 xlsx in raw_data/, found {len(matches)}: {matches}"
        )
    return matches[0]


def _prev_month(year_month: str) -> str:
    """'YYYY-MM' → previous month as 'YYYY-MM'. Handles January wrap."""
    year, month = map(int, year_month.split("-"))
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year}-{month:02d}"


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------


def _validate_row(raw_dict: dict, row_index: int) -> ValidatedRow | None:
    """Validate a single raw row.  Returns None if structurally invalid (dropped)."""
    # --- structural validation via pydantic ---
    # Coerce month to str in case openpyxl returned something else
    if "month" in raw_dict and raw_dict["month"] is not None:
        raw_dict["month"] = str(raw_dict["month"]).strip()

    try:
        raw = RawRow(**raw_dict)
    except Exception as e:
        logger.warning("Row %d: structural validation failed — %s. Dropped.", row_index, e)
        return None

    flags: list[str] = []
    publishable = True

    # --- state resolution (authoritative via state_code) ---
    code_upper = raw.state_code.strip().upper()
    state_ref = states_module.get_state_by_code(code_upper)
    if state_ref is None:
        flags.append(f"unknown_state_code: {code_upper}")
        publishable = False
        # Still build a ValidatedRow so it shows up in qa_summary
        canonical_name = raw.state.strip()
    else:
        canonical_name = state_ref["name"]
        if raw.state.strip() != canonical_name:
            flags.append(f"state_name_normalized: original='{raw.state.strip()}' canonical='{canonical_name}'")

    # --- date normalization ---
    month_canonical = _normalize_date(raw.month)
    if month_canonical is None:
        flags.append(f"unparseable_date: '{raw.month}'")
        publishable = False
        month_canonical = raw.month  # preserve original for the record
    elif raw.month.strip() != month_canonical:
        flags.append(f"date_corrected: original='{raw.month.strip()}' canonical='{month_canonical}'")

    # --- rate plausibility ---
    if raw.unemployment_rate is None:
        flags.append("missing_rate")
        publishable = False
    else:
        if raw.unemployment_rate < IMPLAUSIBLE_RATE_LOWER_BOUND or raw.unemployment_rate >= IMPLAUSIBLE_RATE_UPPER_BOUND:
            flags.append(f"implausible_rate: {raw.unemployment_rate}")
            publishable = False
        elif raw.unemployment_rate >= RATE_WARNING_THRESHOLD:
            flags.append(f"rate_unusually_high: {raw.unemployment_rate}")
            # warning only — stays publishable

    # --- null prev_month (informational) ---
    if raw.unemployment_rate_prev_month is None:
        flags.append("missing_prev_month")

    return ValidatedRow(
        state_canonical=canonical_name,
        state_code=code_upper,
        month_canonical=month_canonical,
        unemployment_rate=raw.unemployment_rate,
        unemployment_rate_prev_month=raw.unemployment_rate_prev_month,
        source=raw.source,
        source_row_index=row_index,
        qa_flags=flags,
        is_publishable=publishable,
    )


def _check_rate_conflicts(validated: list[ValidatedRow]) -> None:
    """Cross-row: block any row whose current rate disagrees with another row's
    prev_month referencing the same (state_code, month).  The later month's
    prev_month is treated as the authoritative revision.

    Mutates is_publishable and qa_flags in place.
    """
    from collections import defaultdict

    # Collect prev_month claims only from currently-publishable rows.
    # Key: (state_code, referenced_month).  Value: list of (value, source_row_index).
    prev_month_claims: dict[tuple[str, str], list[tuple[float, int]]] = defaultdict(list)
    for row in validated:
        if not row.is_publishable or row.unemployment_rate_prev_month is None:
            continue
        try:
            ref_month = _prev_month(row.month_canonical)
        except (ValueError, IndexError):
            continue
        prev_month_claims[(row.state_code, ref_month)].append(
            (row.unemployment_rate_prev_month, row.source_row_index)
        )

    # Check each publishable row's current rate against claims for its month.
    for row in validated:
        if not row.is_publishable or row.unemployment_rate is None:
            continue
        key = (row.state_code, row.month_canonical)
        for claimed_value, claiming_row_idx in prev_month_claims.get(key, []):
            if row.unemployment_rate != claimed_value:
                row.qa_flags.append(
                    f"rate_conflict: current={row.unemployment_rate} "
                    f"vs prev_month={claimed_value} (from source row {claiming_row_idx})"
                )
                row.is_publishable = False
                break  # one conflict flag per row is sufficient


def _check_prev_month_imputed(validated: list[ValidatedRow]) -> None:
    """Warning flag: for rows with missing_prev_month, detect whether the
    previous month's value will still be available in the pivot (from another
    publishable row's current rate or prev_month).  If so, flag as imputed.
    """
    # Set of (state_code, month) that will have a long-format value after pivot.
    will_have_value: set[tuple[str, str]] = set()
    for row in validated:
        if not row.is_publishable:
            continue
        if row.unemployment_rate is not None:
            will_have_value.add((row.state_code, row.month_canonical))
        if row.unemployment_rate_prev_month is not None:
            try:
                will_have_value.add((row.state_code, _prev_month(row.month_canonical)))
            except (ValueError, IndexError):
                pass

    for row in validated:
        if not row.is_publishable or "missing_prev_month" not in row.qa_flags:
            continue
        try:
            prev = _prev_month(row.month_canonical)
        except (ValueError, IndexError):
            continue
        if (row.state_code, prev) in will_have_value:
            row.qa_flags.append(f"prev_month_imputed: sourced from {prev}")


def _check_publish_gate(validated: list[ValidatedRow]) -> bool:
    """Return True if the pipeline may proceed; False if gate trips (abort)."""
    # Group by state_code; a state is "fully unpublishable" if every one of its
    # rows is not publishable.
    from collections import defaultdict
    state_rows: dict[str, list[ValidatedRow]] = defaultdict(list)
    for row in validated:
        state_rows[row.state_code].append(row)

    fully_unpublishable = sum(
        1 for rows in state_rows.values() if all(not r.is_publishable for r in rows)
    )

    fraction = fully_unpublishable / TOTAL_STATES
    if fraction > PUBLISH_GATE_THRESHOLD:
        logger.error(
            "PUBLISH GATE TRIPPED: %d of %d states fully unpublishable (%.0f%% > %.0f%% threshold)",
            fully_unpublishable,
            TOTAL_STATES,
            fraction * 100,
            PUBLISH_GATE_THRESHOLD * 100,
        )
        return False

    logger.info("Publish gate passed: %d states fully unpublishable (%.0f%%)", fully_unpublishable, fraction * 100)
    return True


def _qa_summary(validated: list[ValidatedRow]) -> dict[str, int]:
    """Tally qa_flags across all rows."""
    from collections import Counter
    counts: Counter[str] = Counter()
    for row in validated:
        for flag in row.qa_flags:
            # Normalize flag key (strip the value after the colon for grouping)
            key = flag.split(":")[0]
            counts[key] += 1
    return dict(counts)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


def run_validation(
    input_path: str | None = None,
    run_id: str | None = None,
    pipeline_state_dir: str = ".pipeline_state",
    validated_data_dir: str = "validated_data",
) -> tuple[list[ValidatedRow], bool, str]:
    """Validate the input xlsx.

    Returns:
        (validated_rows, gate_passed, latest_data_month)

    Writes validate_output.json to pipeline_state_dir.
    Raises SystemExit if gate trips and we are in standalone mode.
    """
    if input_path is None:
        input_path = _find_input_file()
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("validate: reading %s", input_path)
    raw_records = _read_xlsx(input_path)
    logger.info("validate: %d raw rows read", len(raw_records))

    validated: list[ValidatedRow] = []
    for i, rec in enumerate(raw_records, start=2):  # row 1 is header → data starts at 2
        result = _validate_row(rec, i)
        if result is not None:
            validated.append(result)

    logger.info("validate: %d rows passed structural validation", len(validated))

    # --- cross-row checks (rate conflicts, imputation detection) ---
    _check_rate_conflicts(validated)
    _check_prev_month_imputed(validated)

    # Determine latest data month from validated rows
    publishable_months = [r.month_canonical for r in validated if r.is_publishable]
    latest_data_month = max(publishable_months) if publishable_months else ""

    # Gate check
    gate_passed = _check_publish_gate(validated)

    # Write validated rows to versioned file
    Path(validated_data_dir).mkdir(parents=True, exist_ok=True)
    rows_filename = f"validated_rows_{latest_data_month}_{run_id}.json"
    rows_path = Path(validated_data_dir) / rows_filename
    rows_path.write_text(json.dumps([r.model_dump() for r in validated], indent=2))
    logger.info("validate: wrote %s (%d rows)", rows_path, len(validated))

    # Write metadata manifest (no row data)
    Path(pipeline_state_dir).mkdir(parents=True, exist_ok=True)
    manifest_payload = {
        "run_id": run_id,
        "produced_at": datetime.now().isoformat(),
        "latest_data_month": latest_data_month,
        "rows_file": str(rows_path),
        "qa_summary": _qa_summary(validated),
    }
    output_path = Path(pipeline_state_dir) / "validate_output.json"
    output_path.write_text(json.dumps(manifest_payload, indent=2))
    logger.info("validate: wrote %s", output_path)

    return validated, gate_passed, latest_data_month


# ---------------------------------------------------------------------------
# Standalone entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    input_file = None
    if "--input" in sys.argv:
        idx = sys.argv.index("--input")
        if idx + 1 < len(sys.argv):
            input_file = sys.argv[idx + 1]
    validated, gate_passed, latest_month = run_validation(input_path=input_file)

    publishable_count = sum(1 for r in validated if r.is_publishable)
    logger.info(
        "validate: %d total validated, %d publishable, latest month = %s",
        len(validated),
        publishable_count,
        latest_month,
    )

    if not gate_passed:
        logger.error("validate: pipeline ABORTED by publish gate.")
        sys.exit(1)

    logger.info("validate: ready for clean step.")
