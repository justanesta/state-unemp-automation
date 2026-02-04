"""Step 2 – Filter, deduplicate, pivot to long format, persist to JSONL.

Standalone: python clean.py [--run-id YYYYMMDD_HHMMSS]
Module:     from clean import run_clean
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from validate import CleanRow, ValidatedRow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prev_month_date(year_month: str) -> str:
    """Given 'YYYY-MM', return the first of the preceding month as 'YYYY-MM-DD'.

    Handles January → December wrap (year decrements).
    """
    year, month = map(int, year_month.split("-"))
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year}-{month:02d}-01"


def _dedupe(rows: list[dict], key_fields: tuple[str, ...]) -> list[dict]:
    """Deduplicate a list of dicts by a composite key.  Returns first occurrence."""
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for row in rows:
        key = tuple(row.get(f) for f in key_fields)
        if key in seen:
            logger.info("clean: deduped row (key=%s)", key)
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def _pivot_row(row: ValidatedRow, ingest_run: str) -> list[CleanRow]:
    """Pivot one wide ValidatedRow into 1-2 long-format CleanRows."""
    results: list[CleanRow] = []

    # Current-month row — always emitted (rate is non-None because is_publishable)
    current_date = f"{row.month_canonical}-01"
    results.append(CleanRow(
        state_canonical=row.state_canonical,
        state_code=row.state_code,
        date=current_date,
        value=row.unemployment_rate,  # type: ignore[arg-type]  # guaranteed non-None for publishable rows
        source=row.source,
        ingest_run=ingest_run,
        source_row_index=row.source_row_index,
    ))

    # Prev-month row — only if prev value is present
    if row.unemployment_rate_prev_month is not None:
        prev_date = _prev_month_date(row.month_canonical)
        results.append(CleanRow(
            state_canonical=row.state_canonical,
            state_code=row.state_code,
            date=prev_date,
            value=row.unemployment_rate_prev_month,
            source=row.source,
            ingest_run=ingest_run,
            source_row_index=row.source_row_index,
        ))

    return results


def run_clean(
    validated_rows: list[ValidatedRow] | None = None,
    run_id: str | None = None,
    pipeline_state_dir: str = ".pipeline_state",
    clean_data_dir: str = "clean_data",
) -> list[CleanRow]:
    """Filter, deduplicate, pivot, and append to the versioned JSONL.

    Args:
        validated_rows: If None, reads from pipeline_state_dir/validate_output.json.
        run_id:         Pipeline run identifier; defaults to current timestamp.

    Returns:
        List of CleanRow objects appended this run.
    """
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    ingest_run_ts = datetime.now().isoformat()

    # --- load from disk if not passed in-process ---
    if validated_rows is None:
        manifest_path = Path(pipeline_state_dir) / "validate_output.json"
        manifest = json.loads(manifest_path.read_text())
        rows_path = Path(manifest["rows_file"])
        logger.info("clean: reading validated rows from %s", rows_path)
        rows_data = json.loads(rows_path.read_text())
        validated_rows = [ValidatedRow(**r) for r in rows_data]

    # --- filter to publishable rows only ---
    publishable = [r for r in validated_rows if r.is_publishable]
    logger.info("clean: %d publishable rows (of %d total)", len(publishable), len(validated_rows))

    # --- deduplicate input rows (before pivot) ---
    input_dicts = [r.model_dump() for r in publishable]
    deduped_input = _dedupe(input_dicts, ("state_code", "month_canonical", "unemployment_rate", "unemployment_rate_prev_month"))
    rows_deduped_input = len(publishable) - len(deduped_input)
    if rows_deduped_input:
        logger.info("clean: deduped %d input rows before pivot", rows_deduped_input)
    deduped_validated = [ValidatedRow(**d) for d in deduped_input]

    # --- pivot to long format with last-write-wins on (state_code, date) ---
    # Sort by month ascending so that later months' prev_month values (which are
    # data revisions) correctly override earlier months' current values.
    # Example: if Nov's rate is 27.0 (typo) but Dec's prev_month says Nov was 5.3,
    # the Dec-sourced value wins because Dec is processed after Nov.
    sorted_validated = sorted(deduped_validated, key=lambda r: r.month_canonical)
    pivoted_lookup: dict[tuple[str, str], CleanRow] = {}
    revisions = 0
    for row in sorted_validated:
        for clean_row in _pivot_row(row, ingest_run_ts):
            key = (clean_row.state_code, clean_row.date)
            if key in pivoted_lookup and pivoted_lookup[key].value != clean_row.value:
                logger.info(
                    "clean: revision for %s %s: %.1f → %.1f (source row %d)",
                    key[0], key[1], pivoted_lookup[key].value, clean_row.value, clean_row.source_row_index,
                )
                revisions += 1
            pivoted_lookup[key] = clean_row
    final_rows = list(pivoted_lookup.values())
    logger.info("clean: pivot produced %d unique state-month rows (%d revisions applied)", len(final_rows), revisions)

    # --- append to versioned JSONL ---
    Path(clean_data_dir).mkdir(parents=True, exist_ok=True)
    jsonl_path = Path(clean_data_dir) / "clean_data.jsonl"
    with jsonl_path.open("a") as fh:
        for row in final_rows:
            fh.write(row.model_dump_json() + "\n")
    logger.info("clean: appended %d rows to %s", len(final_rows), jsonl_path)

    # --- write clean_output manifest ---
    dates = [r.date for r in final_rows]
    Path(pipeline_state_dir).mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "produced_at": datetime.now().isoformat(),
        "rows_appended": len(final_rows),
        "rows_deduped_input": rows_deduped_input,
        "revisions_applied": revisions,
        "date_range": {"min": min(dates) if dates else None, "max": max(dates) if dates else None},
        "states_covered": sorted(set(r.state_code for r in final_rows)),
    }
    (Path(pipeline_state_dir) / "clean_output.json").write_text(json.dumps(manifest, indent=2))
    logger.info("clean: wrote clean_output.json")

    return final_rows


# ---------------------------------------------------------------------------
# Standalone entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    run_id = None
    if "--run-id" in sys.argv:
        idx = sys.argv.index("--run-id")
        if idx + 1 < len(sys.argv):
            run_id = sys.argv[idx + 1]
    clean_rows = run_clean(run_id=run_id)
    logger.info("clean: done. %d rows in clean_data.", len(clean_rows))
