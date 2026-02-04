"""Tests for clean.py – pivot, revision override, dedup, January wrap."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clean import _pivot_row, _prev_month_date, run_clean  # noqa: E402
from validate import CleanRow, ValidatedRow  # noqa: E402


# ---------------------------------------------------------------------------
# Prev-month date inference
# ---------------------------------------------------------------------------


class TestPrevMonthDate:
    def test_normal_month(self):
        assert _prev_month_date("2025-12") == "2025-11-01"

    def test_january_wraps_to_december(self):
        assert _prev_month_date("2025-01") == "2024-12-01"

    def test_february(self):
        assert _prev_month_date("2025-02") == "2025-01-01"


# ---------------------------------------------------------------------------
# Pivot
# ---------------------------------------------------------------------------


def _row(state_code: str, month: str, rate: float, prev: float | None, source_idx: int = 2) -> ValidatedRow:
    return ValidatedRow(
        state_canonical=state_code, state_code=state_code, month_canonical=month,
        unemployment_rate=rate, unemployment_rate_prev_month=prev,
        source="BLS", source_row_index=source_idx, qa_flags=[], is_publishable=True,
    )


class TestPivot:
    def test_both_rates_produce_two_rows(self):
        result = _pivot_row(_row("AL", "2025-12", 4.6, 4.5), ingest_run="2026-01-01T00:00:00")
        assert len(result) == 2
        dates = {r.date for r in result}
        assert dates == {"2025-12-01", "2025-11-01"}

    def test_none_prev_produces_one_row(self):
        result = _pivot_row(_row("AL", "2025-12", 4.6, None), ingest_run="2026-01-01T00:00:00")
        assert len(result) == 1
        assert result[0].date == "2025-12-01"

    def test_pivot_values_correct(self):
        result = _pivot_row(_row("AL", "2025-12", 4.6, 4.5), ingest_run="ts")
        by_date = {r.date: r for r in result}
        assert by_date["2025-12-01"].value == 4.6
        assert by_date["2025-11-01"].value == 4.5

    def test_january_pivot_wraps_year(self):
        result = _pivot_row(_row("AL", "2025-01", 5.0, 4.8), ingest_run="ts")
        dates = {r.date for r in result}
        assert "2024-12-01" in dates


# ---------------------------------------------------------------------------
# run_clean – revision override and dedup
# ---------------------------------------------------------------------------


class TestRunClean:
    def test_basic_pivot_and_append(self, tmp_pipeline: dict[str, str]) -> None:
        rows = [_row("AL", "2025-12", 4.6, 4.5)]
        result = run_clean(
            validated_rows=rows, run_id="test1",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        assert len(result) == 2
        jsonl_path = Path(tmp_pipeline["clean_data"]) / "clean_data.jsonl"
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_revision_overrides_earlier_month(self, tmp_pipeline: dict[str, str]) -> None:
        """Nov row says rate=10.0; Dec row says prev_month (Nov) was 5.0.
        After sorted pivot, Nov ends up as 5.0 (Dec's prev overrides)."""
        rows = [
            _row("AL", "2025-11", 10.0, 9.0, source_idx=2),   # Nov current = 10.0
            _row("AL", "2025-12", 4.6, 5.0, source_idx=3),    # Dec prev (Nov) = 5.0
        ]
        result = run_clean(
            validated_rows=rows, run_id="test2",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        nov_rows = [r for r in result if r.date == "2025-11-01"]
        assert len(nov_rows) == 1
        assert nov_rows[0].value == 5.0  # Dec's prev_month wins

    def test_input_duplicates_are_deduped(self, tmp_pipeline: dict[str, str]) -> None:
        """Exact duplicate input rows produce no duplicate output."""
        rows = [
            _row("AL", "2025-12", 4.6, 4.5),
            _row("AL", "2025-12", 4.6, 4.5),  # exact dupe
        ]
        result = run_clean(
            validated_rows=rows, run_id="test3",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        dec_rows = [r for r in result if r.date == "2025-12-01"]
        assert len(dec_rows) == 1

    def test_append_is_additive_across_runs(self, tmp_pipeline: dict[str, str]) -> None:
        """Two runs append to the same JSONL; file grows."""
        run_clean(
            validated_rows=[_row("AL", "2025-11", 4.5, 4.4)],
            run_id="run_a",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        run_clean(
            validated_rows=[_row("AK", "2025-12", 5.8, 5.6)],
            run_id="run_b",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        jsonl_path = Path(tmp_pipeline["clean_data"]) / "clean_data.jsonl"
        lines = jsonl_path.read_text().strip().split("\n")
        # run_a: AL Nov + AL Oct = 2 rows; run_b: AK Dec + AK Nov = 2 rows
        assert len(lines) == 4

    def test_unpublishable_rows_filtered(self, tmp_pipeline: dict[str, str]) -> None:
        bad = ValidatedRow(
            state_canonical="CO", state_code="CO", month_canonical="2025-10",
            unemployment_rate=-1.2, unemployment_rate_prev_month=3.0,
            source="BLS", source_row_index=2,
            qa_flags=["implausible_rate: -1.2"], is_publishable=False,
        )
        good = _row("AL", "2025-12", 4.6, 4.5)
        result = run_clean(
            validated_rows=[bad, good], run_id="test4",
            pipeline_state_dir=tmp_pipeline["pipeline_state"],
            clean_data_dir=tmp_pipeline["clean_data"],
        )
        codes = {r.state_code for r in result}
        assert "CO" not in codes
        assert "AL" in codes
