"""Pipeline orchestrator – runs validate → clean → output end-to-end.

Usage: uv run python main.py
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import clean as clean_module
import output as output_module
import validate as validate_module

logger = logging.getLogger(__name__)

PIPELINE_STATE_DIR = ".pipeline_state"


def _write_manifest(run_id: str, data: dict) -> None:
    Path(PIPELINE_STATE_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(PIPELINE_STATE_DIR) / "run_manifest.json"
    path.write_text(json.dumps(data, indent=2))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("=== pipeline start  run_id=%s ===", run_id)

    # --- initial manifest ---
    manifest: dict = {
        "run_id": run_id,
        "started_at": datetime.now().isoformat(),
        "status": "started",
        "steps_completed": [],
        "latest_data_month": None,
        "input_file": None,
        "rows_ingested": None,
        "rows_validated": None,
        "rows_publishable": None,
        "states_with_data": None,
        "gate_passed": None,
        "abort_reason": None,
    }
    _write_manifest(run_id, manifest)

    # -----------------------------------------------------------------------
    # Step 1 – validate
    # -----------------------------------------------------------------------
    manifest["status"] = "validating"
    _write_manifest(run_id, manifest)

    validated_rows, gate_passed, latest_data_month = validate_module.run_validation(run_id=run_id)

    publishable_count = sum(1 for r in validated_rows if r.is_publishable)
    state_set: set[str] = set()
    for r in validated_rows:
        if r.is_publishable:
            state_set.add(r.state_code)

    manifest["steps_completed"].append("validate")
    manifest["rows_ingested"] = len(validated_rows)
    manifest["rows_validated"] = len(validated_rows)
    manifest["rows_publishable"] = publishable_count
    manifest["states_with_data"] = len(state_set)
    manifest["latest_data_month"] = latest_data_month
    manifest["gate_passed"] = gate_passed

    if not gate_passed:
        manifest["status"] = "ABORTED"
        manifest["abort_reason"] = "Publish gate tripped: too many states fully unpublishable."
        _write_manifest(run_id, manifest)
        logger.error("=== pipeline ABORTED ===")
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Step 2 – clean
    # -----------------------------------------------------------------------
    manifest["status"] = "cleaning"
    _write_manifest(run_id, manifest)

    clean_rows = clean_module.run_clean(validated_rows=validated_rows, run_id=run_id)

    manifest["steps_completed"].append("clean")
    logger.info("clean: %d rows written to JSONL", len(clean_rows))

    # -----------------------------------------------------------------------
    # Step 3 – output
    # -----------------------------------------------------------------------
    manifest["status"] = "outputting"
    _write_manifest(run_id, manifest)

    output_module.run_output(run_id=run_id, latest_data_month=latest_data_month)

    manifest["steps_completed"].append("output")
    manifest["status"] = "completed"
    _write_manifest(run_id, manifest)

    logger.info("=== pipeline complete  run_id=%s ===", run_id)


if __name__ == "__main__":
    main()
