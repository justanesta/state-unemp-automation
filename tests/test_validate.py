"""Tests for validate.py – date normalization, state resolution, rate checks, gate."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from validate import (  # noqa: E402
    ValidatedRow,
    _check_prev_month_imputed,
    _check_publish_gate,
    _check_rate_conflicts,
    _normalize_date,
    _validate_row,
)


# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------


class TestNormalizeDate:
    def test_hyphen_format(self):
        assert _normalize_date("2025-12") == "2025-12"

    def test_slash_format(self):
        assert _normalize_date("2025/10") == "2025-10"

    def test_with_whitespace(self):
        assert _normalize_date("  2025-09  ") == "2025-09"

    def test_unparseable(self):
        assert _normalize_date("foobar") is None

    def test_partial_date(self):
        assert _normalize_date("2025") is None


# ---------------------------------------------------------------------------
# Row validation – state resolution
# ---------------------------------------------------------------------------


class TestStateResolution:
    def test_canonical_name_unchanged(self):
        row = _validate_row({
            "state": "California", "state_code": "CA", "month": "2025-12",
            "unemployment_rate": 4.0, "unemployment_rate_prev_month": 4.1, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.state_canonical == "California"
        assert row.qa_flags == []

    def test_abbreviated_name_normalized(self):
        row = _validate_row({
            "state": "Calif.", "state_code": "CA", "month": "2025-12",
            "unemployment_rate": 4.0, "unemployment_rate_prev_month": 4.1, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.state_canonical == "California"
        assert any("state_name_normalized" in f for f in row.qa_flags)

    def test_code_as_name(self):
        row = _validate_row({
            "state": "TX", "state_code": "TX", "month": "2025-11",
            "unemployment_rate": 5.0, "unemployment_rate_prev_month": 5.1, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.state_canonical == "Texas"
        assert any("state_name_normalized" in f for f in row.qa_flags)

    def test_unknown_state_code(self):
        row = _validate_row({
            "state": "Nowhere", "state_code": "XX", "month": "2025-12",
            "unemployment_rate": 3.0, "unemployment_rate_prev_month": 3.1, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is False
        assert any("unknown_state_code" in f for f in row.qa_flags)


# ---------------------------------------------------------------------------
# Row validation – rate plausibility
# ---------------------------------------------------------------------------


class TestRatePlausibility:
    def test_negative_rate_blocked(self):
        row = _validate_row({
            "state": "Colorado", "state_code": "CO", "month": "2025-10",
            "unemployment_rate": -1.2, "unemployment_rate_prev_month": 3.0, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is False
        assert any("implausible_rate" in f for f in row.qa_flags)

    def test_rate_at_100_blocked(self):
        row = _validate_row({
            "state": "Colorado", "state_code": "CO", "month": "2025-10",
            "unemployment_rate": 100.0, "unemployment_rate_prev_month": 3.0, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is False

    def test_rate_just_below_100_publishable(self):
        row = _validate_row({
            "state": "Colorado", "state_code": "CO", "month": "2025-10",
            "unemployment_rate": 99.9, "unemployment_rate_prev_month": 3.0, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is True
        assert any("rate_unusually_high" in f for f in row.qa_flags)

    def test_warning_flag_for_high_rate(self):
        row = _validate_row({
            "state": "Rhode Island", "state_code": "RI", "month": "2025-11",
            "unemployment_rate": 27.0, "unemployment_rate_prev_month": 5.2, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is True  # below 100
        assert any("rate_unusually_high" in f for f in row.qa_flags)

    def test_none_rate_blocked(self):
        row = _validate_row({
            "state": "Alabama", "state_code": "AL", "month": "2025-12",
            "unemployment_rate": None, "unemployment_rate_prev_month": 4.5, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is False
        assert any("missing_rate" in f for f in row.qa_flags)

    def test_none_prev_month_still_publishable(self):
        row = _validate_row({
            "state": "Alabama", "state_code": "AL", "month": "2025-12",
            "unemployment_rate": 4.5, "unemployment_rate_prev_month": None, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is True
        assert any("missing_prev_month" in f for f in row.qa_flags)


# ---------------------------------------------------------------------------
# Publish gate
# ---------------------------------------------------------------------------


def _make_unpublishable_row(state_code: str, month: str) -> ValidatedRow:
    return ValidatedRow(
        state_canonical=state_code, state_code=state_code, month_canonical=month,
        unemployment_rate=None, unemployment_rate_prev_month=None,
        source="BLS", source_row_index=0, qa_flags=["missing_rate"], is_publishable=False,
    )


def _make_publishable_row(state_code: str, month: str) -> ValidatedRow:
    return ValidatedRow(
        state_canonical=state_code, state_code=state_code, month_canonical=month,
        unemployment_rate=4.0, unemployment_rate_prev_month=4.1,
        source="BLS", source_row_index=0, qa_flags=[], is_publishable=True,
    )


class TestPublishGate:
    def test_gate_passes_with_zero_unpublishable(self):
        rows = [_make_publishable_row(f"S{i:02d}", "2025-12") for i in range(50)]
        assert _check_publish_gate(rows) is True

    def test_gate_trips_at_threshold(self):
        # 21 fully unpublishable states > 40% of 50
        rows: list[ValidatedRow] = []
        for i in range(21):
            rows.append(_make_unpublishable_row(f"S{i:02d}", "2025-12"))
        for i in range(21, 50):
            rows.append(_make_publishable_row(f"S{i:02d}", "2025-12"))
        assert _check_publish_gate(rows) is False

    def test_gate_passes_just_under_threshold(self):
        # 19 fully unpublishable = 38% < 40%
        rows: list[ValidatedRow] = []
        for i in range(19):
            rows.append(_make_unpublishable_row(f"S{i:02d}", "2025-12"))
        for i in range(19, 50):
            rows.append(_make_publishable_row(f"S{i:02d}", "2025-12"))
        assert _check_publish_gate(rows) is True

    def test_state_with_one_good_month_is_not_fully_unpublishable(self):
        # S00 has one bad month and one good month → not fully unpublishable
        rows: list[ValidatedRow] = [
            _make_unpublishable_row("S00", "2025-11"),
            _make_publishable_row("S00", "2025-12"),
        ]
        for i in range(1, 50):
            rows.append(_make_publishable_row(f"S{i:02d}", "2025-12"))
        assert _check_publish_gate(rows) is True


# ---------------------------------------------------------------------------
# Date-correction flag
# ---------------------------------------------------------------------------


class TestDateCorrected:
    def test_slash_date_produces_flag(self):
        row = _validate_row({
            "state": "Alabama", "state_code": "AL", "month": "2025/12",
            "unemployment_rate": 4.5, "unemployment_rate_prev_month": 4.6, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert row.is_publishable is True
        assert row.month_canonical == "2025-12"
        assert any("date_corrected" in f for f in row.qa_flags)

    def test_hyphen_date_no_flag(self):
        row = _validate_row({
            "state": "Alabama", "state_code": "AL", "month": "2025-12",
            "unemployment_rate": 4.5, "unemployment_rate_prev_month": 4.6, "source": "BLS",
        }, row_index=2)
        assert row is not None
        assert not any("date_corrected" in f for f in row.qa_flags)


# ---------------------------------------------------------------------------
# Cross-row rate conflict
# ---------------------------------------------------------------------------


class TestRateConflict:
    def test_conflict_blocks_earlier_row(self):
        """RI Nov current (27.0) vs RI Dec prev_month (5.3) → Nov blocked."""
        rows = [
            ValidatedRow(
                state_canonical="Rhode Island", state_code="RI", month_canonical="2025-11",
                unemployment_rate=27.0, unemployment_rate_prev_month=5.2,
                source="BLS", source_row_index=2,
                qa_flags=["rate_unusually_high: 27.0"], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Rhode Island", state_code="RI", month_canonical="2025-12",
                unemployment_rate=5.5, unemployment_rate_prev_month=5.3,
                source="BLS", source_row_index=3, qa_flags=[], is_publishable=True,
            ),
        ]
        _check_rate_conflicts(rows)
        assert rows[0].is_publishable is False
        assert any("rate_conflict" in f for f in rows[0].qa_flags)
        # Dec row is unaffected
        assert rows[1].is_publishable is True
        assert not any("rate_conflict" in f for f in rows[1].qa_flags)

    def test_no_conflict_when_rates_agree(self):
        """Nov current matches Dec prev_month → both stay publishable."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-11",
                unemployment_rate=4.5, unemployment_rate_prev_month=4.4,
                source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=4.5,
                source="BLS", source_row_index=3, qa_flags=[], is_publishable=True,
            ),
        ]
        _check_rate_conflicts(rows)
        assert rows[0].is_publishable is True
        assert rows[1].is_publishable is True

    def test_no_conflict_when_later_row_has_no_prev_month(self):
        """Dec prev_month is None → no claim exists to check Nov against."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-11",
                unemployment_rate=4.5, unemployment_rate_prev_month=4.4,
                source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=None,
                source="BLS", source_row_index=3,
                qa_flags=["missing_prev_month"], is_publishable=True,
            ),
        ]
        _check_rate_conflicts(rows)
        assert rows[0].is_publishable is True

    def test_unpublishable_source_does_not_contribute_claims(self):
        """An already-unpublishable row's prev_month is ignored as a claim source."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-11",
                unemployment_rate=4.5, unemployment_rate_prev_month=4.4,
                source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.0, unemployment_rate_prev_month=9.9,
                source="BLS", source_row_index=3,
                qa_flags=["missing_rate"], is_publishable=False,
            ),
        ]
        _check_rate_conflicts(rows)
        # Nov should NOT be blocked — the conflicting claim came from an unpublishable row
        assert rows[0].is_publishable is True


# ---------------------------------------------------------------------------
# Prev-month imputation detection
# ---------------------------------------------------------------------------


class TestPrevMonthImputed:
    def test_imputed_when_source_month_available(self):
        """Dec prev_month is None but Nov has a current rate → imputed."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-11",
                unemployment_rate=4.5, unemployment_rate_prev_month=4.4,
                source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=None,
                source="BLS", source_row_index=3,
                qa_flags=["missing_prev_month"], is_publishable=True,
            ),
        ]
        _check_prev_month_imputed(rows)
        assert any("prev_month_imputed" in f for f in rows[1].qa_flags)

    def test_not_imputed_when_no_source(self):
        """Dec prev_month is None and no Nov row exists → not imputed."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=None,
                source="BLS", source_row_index=2,
                qa_flags=["missing_prev_month"], is_publishable=True,
            ),
        ]
        _check_prev_month_imputed(rows)
        assert not any("prev_month_imputed" in f for f in rows[0].qa_flags)

    def test_no_flag_when_prev_month_present(self):
        """Row without missing_prev_month is not touched."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=4.5,
                source="BLS", source_row_index=2, qa_flags=[], is_publishable=True,
            ),
        ]
        _check_prev_month_imputed(rows)
        assert not any("prev_month_imputed" in f for f in rows[0].qa_flags)

    def test_imputed_via_another_rows_prev_month(self):
        """No Nov current-rate row, but another Dec row has prev_month covering Nov
        → Nov will have a pivot value, so the missing prev is imputed."""
        rows = [
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.6, unemployment_rate_prev_month=None,
                source="BLS", source_row_index=2,
                qa_flags=["missing_prev_month"], is_publishable=True,
            ),
            ValidatedRow(
                state_canonical="Alabama", state_code="AL", month_canonical="2025-12",
                unemployment_rate=4.7, unemployment_rate_prev_month=4.5,
                source="BLS", source_row_index=3, qa_flags=[], is_publishable=True,
            ),
        ]
        _check_prev_month_imputed(rows)
        assert any("prev_month_imputed" in f for f in rows[0].qa_flags)
