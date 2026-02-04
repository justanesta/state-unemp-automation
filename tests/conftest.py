"""Shared fixtures for pipeline tests."""

import json
import sys
from pathlib import Path

import pytest

# Ensure repo root is on sys.path so imports like `import states` work
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validate import ValidatedRow  # noqa: E402


@pytest.fixture
def sample_validated_rows() -> list[ValidatedRow]:
    """Four rows for two states across two months; clean and publishable."""
    return [
        ValidatedRow(
            state_canonical="Alabama", state_code="AL", month_canonical="2025-11",
            unemployment_rate=4.5, unemployment_rate_prev_month=4.6,
            source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
        ),
        ValidatedRow(
            state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
            unemployment_rate=4.6, unemployment_rate_prev_month=4.5,
            source="BLS", source_row_index=3, qa_flags=[], is_publishable=True,
        ),
        ValidatedRow(
            state_canonical="Alaska", state_code="AK", month_canonical="2025-11",
            unemployment_rate=5.6, unemployment_rate_prev_month=5.5,
            source="BLS", source_row_index=4, qa_flags=[], is_publishable=True,
        ),
        ValidatedRow(
            state_canonical="Alaska", state_code="AK", month_canonical="2025-12",
            unemployment_rate=5.8, unemployment_rate_prev_month=5.6,
            source="BLS", source_row_index=5, qa_flags=[], is_publishable=True,
        ),
    ]


@pytest.fixture
def tmp_pipeline(tmp_path: Path) -> dict[str, str]:
    """Return paths for a temporary pipeline layout."""
    dirs = {
        "clean_data": str(tmp_path / "clean_data"),
        "pipeline_state": str(tmp_path / ".pipeline_state"),
    }
    for d in dirs.values():
        Path(d).mkdir(parents=True, exist_ok=True)
    return dirs


def _write_jsonl(path: str, rows: list[dict]) -> None:
    with open(path, "a") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
