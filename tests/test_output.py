"""Tests for output.py – ordinal, AP dates, rankings, MoM, templates."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from output import (  # noqa: E402
    _competition_ranks,
    _format_ap_date,
    _render_summary,
    _scoped_ranks,
    ordinal,
)


# ---------------------------------------------------------------------------
# Ordinal helper
# ---------------------------------------------------------------------------


class TestOrdinal:
    def test_basic(self):
        assert ordinal(1) == "1st"
        assert ordinal(2) == "2nd"
        assert ordinal(3) == "3rd"
        assert ordinal(4) == "4th"

    def test_teens(self):
        assert ordinal(11) == "11th"
        assert ordinal(12) == "12th"
        assert ordinal(13) == "13th"

    def test_twenties(self):
        assert ordinal(21) == "21st"
        assert ordinal(22) == "22nd"
        assert ordinal(23) == "23rd"

    def test_hundreds(self):
        assert ordinal(100) == "100th"
        assert ordinal(101) == "101st"
        assert ordinal(102) == "102nd"
        assert ordinal(103) == "103rd"
        assert ordinal(111) == "111th"
        assert ordinal(112) == "112th"
        assert ordinal(113) == "113th"


# ---------------------------------------------------------------------------
# AP Style date formatting
# ---------------------------------------------------------------------------


class TestAPDate:
    def test_abbreviated_month(self):
        assert _format_ap_date("2025-12-01") == "Dec. 1, 2025"
        assert _format_ap_date("2025-09-01") == "Sept. 1, 2025"
        assert _format_ap_date("2025-01-01") == "Jan. 1, 2025"

    def test_non_abbreviated_month(self):
        # March, April, May, June, July are NOT abbreviated in AP Style
        assert _format_ap_date("2025-03-01") == "March 1, 2025"
        assert _format_ap_date("2025-04-01") == "April 1, 2025"
        assert _format_ap_date("2025-05-01") == "May 1, 2025"
        assert _format_ap_date("2025-06-01") == "June 1, 2025"
        assert _format_ap_date("2025-07-01") == "July 1, 2025"

    def test_february(self):
        assert _format_ap_date("2025-02-01") == "Feb. 1, 2025"


# ---------------------------------------------------------------------------
# Competition ranking
# ---------------------------------------------------------------------------


class TestCompetitionRanks:
    def test_no_ties(self):
        items = [("A", 5.0), ("B", 4.0), ("C", 3.0)]
        ranks = _competition_ranks(items)
        assert ranks == {"A": 1, "B": 2, "C": 3}

    def test_two_way_tie(self):
        items = [("A", 5.0), ("B", 5.0), ("C", 3.0)]
        ranks = _competition_ranks(items)
        assert ranks["A"] == 1
        assert ranks["B"] == 1
        assert ranks["C"] == 3  # skips rank 2

    def test_three_way_tie(self):
        items = [("A", 4.0), ("B", 4.0), ("C", 4.0), ("D", 2.0)]
        ranks = _competition_ranks(items)
        assert ranks["A"] == 1
        assert ranks["B"] == 1
        assert ranks["C"] == 1
        assert ranks["D"] == 4

    def test_single_item(self):
        assert _competition_ranks([("X", 3.0)]) == {"X": 1}

    def test_ascending_order(self):
        items = [("A", 1.0), ("B", 2.0), ("C", 3.0)]
        ranks = _competition_ranks(items, reverse=False)
        assert ranks == {"A": 1, "B": 2, "C": 3}


# ---------------------------------------------------------------------------
# Scoped rankings (uses real states.py data)
# ---------------------------------------------------------------------------


class TestScopedRanks:
    def test_two_states_same_division(self):
        # Alabama and Kentucky are both in East South Central
        state_values = {"AL": 5.0, "KY": 4.0}
        ranks = _scoped_ranks(state_values)
        # Both ranked nationally
        assert ranks["AL"]["national"] == 1
        assert ranks["KY"]["national"] == 2
        # Both in same division → divisional ranks 1 and 2
        assert ranks["AL"]["divisional"] == 1
        assert ranks["KY"]["divisional"] == 2

    def test_states_in_different_regions(self):
        # Alabama (South) and Alaska (West)
        state_values = {"AL": 5.0, "AK": 4.0}
        ranks = _scoped_ranks(state_values)
        # Each is alone in their region → both rank 1 regionally
        assert ranks["AL"]["regional"] == 1
        assert ranks["AK"]["regional"] == 1


# ---------------------------------------------------------------------------
# Summary sentence rendering
# ---------------------------------------------------------------------------


class TestRenderSummary:
    def test_up(self):
        s = _render_summary("Alabama", 4.6, "2025-12-01", 0.1, "up")
        assert s == (
            "Alabama's unemployment rate was 4.6 percent in Dec. 1, 2025, "
            "up 0.1 percentage points from the prior month."
        )

    def test_down(self):
        s = _render_summary("Alaska", 5.5, "2025-11-01", -0.3, "down")
        assert s == (
            "Alaska's unemployment rate was 5.5 percent in Nov. 1, 2025, "
            "down 0.3 percentage points from the prior month."
        )

    def test_flat(self):
        s = _render_summary("Colorado", 3.1, "2025-12-01", 0.0, "flat")
        assert s == (
            "Colorado's unemployment rate was 3.1 percent in Dec. 1, 2025, "
            "unchanged from the prior month."
        )

    def test_missing_prev(self):
        s = _render_summary("Texas", 6.0, "2025-10-01", None, None)
        assert s == (
            "Texas's unemployment rate was 6.0 percent in Oct. 1, 2025. "
            "Month-over-month change data is not available."
        )

    def test_whole_number_rate_keeps_decimal(self):
        s = _render_summary("Ohio", 5.0, "2025-12-01", 0.1, "up")
        assert "5.0 percent" in s

    def test_possessive_on_s_ending_state(self):
        # AP Style: always 's, even for names ending in s
        s = _render_summary("Texas", 6.0, "2025-12-01", 0.2, "up")
        assert s.startswith("Texas's ")

        s = _render_summary("Illinois", 5.6, "2025-12-01", 0.1, "up")
        assert s.startswith("Illinois's ")
