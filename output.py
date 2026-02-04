"""Step 3 – Compute MoM/rankings, render templates, emit output files.

Standalone: python output.py [--run-id YYYYMMDD_HHMMSS]
Module:     from output import run_output
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import states as states_module

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AP Style date formatting
# ---------------------------------------------------------------------------

_MONTH_ABBREVS: dict[int, str] = {
    1: "Jan.", 2: "Feb.", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "Aug.",
    9: "Sept.", 10: "Oct.", 11: "Nov.", 12: "Dec.",
}


def _format_ap_date(date_str: str) -> str:
    """'2025-12-01' → 'Dec. 1, 2025'."""
    year, month, day = map(int, date_str.split("-"))
    return f"{_MONTH_ABBREVS[month]} {day}, {year}"


# ---------------------------------------------------------------------------
# Ordinal helper
# ---------------------------------------------------------------------------


def ordinal(n: int) -> str:
    """1 → '1st', 2 → '2nd', 3 → '3rd', 11 → '11th', 21 → '21st', etc."""
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    if n % 10 == 1:
        return f"{n}st"
    if n % 10 == 2:
        return f"{n}nd"
    if n % 10 == 3:
        return f"{n}rd"
    return f"{n}th"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _read_latest_clean_data(path: str = "clean_data/clean_data.jsonl") -> list[dict]:
    """Read JSONL, keep only the latest version per (state_code, date)."""
    fpath = Path(path)
    if not fpath.exists():
        raise FileNotFoundError(f"clean_data not found: {fpath}")

    by_key: dict[tuple[str, str], dict] = {}
    with fpath.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            key = (row["state_code"], row["date"])
            if key not in by_key or row["ingest_run"] > by_key[key]["ingest_run"]:
                by_key[key] = row
    return list(by_key.values())


def _load_qa_flags(pipeline_state_dir: str = ".pipeline_state") -> dict[tuple[str, str], list[str]]:
    """Read validated rows file (path from manifest) → {(state_code, month_canonical): [flags]}."""
    manifest_path = Path(pipeline_state_dir) / "validate_output.json"
    if not manifest_path.exists():
        return {}
    manifest = json.loads(manifest_path.read_text())
    rows_path = Path(manifest["rows_file"])
    if not rows_path.exists():
        return {}
    rows = json.loads(rows_path.read_text())
    result: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        key = (row["state_code"], row["month_canonical"])
        if key in result:
            result[key].extend(row["qa_flags"])
        else:
            result[key] = list(row["qa_flags"])
    return result


# ---------------------------------------------------------------------------
# Prev-month date helper
# ---------------------------------------------------------------------------


def _prev_month_date(date_str: str) -> str:
    """'2025-12-01' → '2025-11-01'. Handles January wrap."""
    year, month, _ = map(int, date_str.split("-"))
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year}-{month:02d}-01"


# ---------------------------------------------------------------------------
# Competition ranking
# ---------------------------------------------------------------------------


def _competition_ranks(items: list[tuple[str, float]], reverse: bool = True) -> dict[str, int]:
    """Rank (key, value) pairs using competition ranking (1,1,3 style).

    reverse=True → rank 1 = highest value.
    """
    sorted_items = sorted(items, key=lambda x: x[1], reverse=reverse)
    ranks: dict[str, int] = {}
    i = 0
    while i < len(sorted_items):
        j = i
        while j < len(sorted_items) and sorted_items[j][1] == sorted_items[i][1]:
            j += 1
        for k in range(i, j):
            ranks[sorted_items[k][0]] = i + 1
        i = j
    return ranks


def _scoped_ranks(state_values: dict[str, float]) -> dict[str, dict[str, int]]:
    """National + regional + divisional competition ranks (all descending).

    Returns {state_code: {"national": rank, "regional": rank, "divisional": rank}}
    States whose region/division is empty are ranked nationally only.
    """
    national = _competition_ranks(list(state_values.items()))

    reg_groups: dict[str, list[tuple[str, float]]] = defaultdict(list)
    div_groups: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for code, val in state_values.items():
        ref = states_module.get_state_by_code(code)
        if ref and ref["census_region"] and ref["census_division"]:
            reg_groups[ref["census_region"]].append((code, val))
            div_groups[ref["census_division"]].append((code, val))

    regional: dict[str, int] = {}
    for group in reg_groups.values():
        regional.update(_competition_ranks(group))

    divisional: dict[str, int] = {}
    for group in div_groups.values():
        divisional.update(_competition_ranks(group))

    return {
        code: {
            "national": national.get(code, 0),
            "regional": regional.get(code, 0),
            "divisional": divisional.get(code, 0),
        }
        for code in state_values
    }


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------


def _render_summary(
    state_name: str,
    rate: float,
    date_str: str,
    mom_change: float | None,
    trend: str | None,
) -> str:
    ap_date = _format_ap_date(date_str)
    rate_str = f"{rate:.1f}"

    if mom_change is None or trend is None:
        return (
            f"{state_name}'s unemployment rate was {rate_str} percent in {ap_date}. "
            f"Month-over-month change data is not available."
        )
    if trend == "flat":
        return (
            f"{state_name}'s unemployment rate was {rate_str} percent in {ap_date}, "
            f"unchanged from the prior month."
        )

    direction = "up" if trend == "up" else "down"
    change_str = f"{abs(mom_change):.1f}"
    return (
        f"{state_name}'s unemployment rate was {rate_str} percent in {ap_date}, "
        f"{direction} {change_str} percentage points from the prior month."
    )


def _render_ranking_paragraph(
    state_name: str,
    date_str: str,
    descriptor: str,
    state_code: str,
    rank_data: dict[str, dict[str, int]],
    state_ref: dict,
    scope_counts: dict[str, dict[str, int]],
) -> str | None:
    """Render a ranking paragraph (rate or MoM).  Returns None if not renderable."""
    region = state_ref.get("census_region", "")
    division = state_ref.get("census_division", "")
    if not region or not division:
        return None
    if state_code not in rank_data:
        return None

    r = rank_data[state_code]
    ap_date = _format_ap_date(date_str)
    n_div = scope_counts["divisional"].get(division, 0)
    n_reg = scope_counts["regional"].get(region, 0)

    return (
        f"In {ap_date}, {state_name} had the {ordinal(r['divisional'])} {descriptor} "
        f"in the {division} division of {n_div} states, "
        f"the {ordinal(r['regional'])} {descriptor} "
        f"in the {region} region of {n_reg} states, "
        f"and the {ordinal(r['national'])} {descriptor} in the country overall."
    )


# ---------------------------------------------------------------------------
# CSV / JSON writers
# ---------------------------------------------------------------------------


def _write_csv(filepath: str, rows: list[dict], fieldnames: list[str]) -> None:
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("output: wrote %s (%d rows)", filepath, len(rows))


def _write_json(filepath: str, data: list[dict]) -> None:
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(json.dumps(data, indent=2))
    logger.info("output: wrote %s (%d entries)", filepath, len(data))


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def run_output(
    run_id: str | None = None,
    latest_data_month: str | None = None,
    pipeline_state_dir: str = ".pipeline_state",
    clean_data_path: str = "clean_data/clean_data.jsonl",
) -> None:
    """Compute MoM, rankings, render templates, and write all output files."""
    # --- resolve run_id / latest_data_month ---
    if run_id is None or latest_data_month is None:
        manifest_path = Path(pipeline_state_dir) / "run_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            run_id = run_id or manifest.get("run_id")
            latest_data_month = latest_data_month or manifest.get("latest_data_month")
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # --- load ---
    clean_rows = _read_latest_clean_data(clean_data_path)
    logger.info("output: %d latest-version rows loaded", len(clean_rows))
    qa_flags_map = _load_qa_flags(pipeline_state_dir)

    # --- value lookup: (state_code, date) → rate ---
    value_lookup: dict[tuple[str, str], float] = {
        (r["state_code"], r["date"]): r["value"] for r in clean_rows
    }

    # --- derive latest_data_month from data if not provided ---
    if not latest_data_month:
        latest_data_month = max(r["date"][:7] for r in clean_rows)
    latest_date = f"{latest_data_month}-01"

    # --- all (state_code, date) pairs grouped by state ---
    state_months: dict[str, list[str]] = defaultdict(list)
    for r in clean_rows:
        state_months[r["state_code"]].append(r["date"])

    # --- MoM for every (state, date) pair ---
    mom_data: dict[tuple[str, str], tuple[float | None, str | None]] = {}
    for code, dates in state_months.items():
        for date in dates:
            prev = _prev_month_date(date)
            curr_val = value_lookup.get((code, date))
            prev_val = value_lookup.get((code, prev))
            if curr_val is not None and prev_val is not None:
                change = round(curr_val - prev_val, 1)
                trend: str | None = "up" if change > 0 else ("down" if change < 0 else "flat")
                mom_data[(code, date)] = (change, trend)
            else:
                mom_data[(code, date)] = (None, None)

    # --- states present in the latest month ---
    latest_states: dict[str, float] = {}
    for code in state_months:
        val = value_lookup.get((code, latest_date))
        if val is not None:
            latest_states[code] = val

    # --- rankings (latest month only) ---
    rate_ranks = _scoped_ranks(latest_states)

    mom_abs_values: dict[str, float] = {}
    for code in latest_states:
        change, _ = mom_data.get((code, latest_date), (None, None))
        if change is not None:
            mom_abs_values[code] = abs(change)
    mom_ranks = _scoped_ranks(mom_abs_values)

    # scope counts — how many states actually have data per region/division
    scope_counts: dict[str, dict[str, int]] = {"regional": {}, "divisional": {}}
    for code in latest_states:
        ref = states_module.get_state_by_code(code)
        if ref and ref["census_region"] and ref["census_division"]:
            scope_counts["regional"][ref["census_region"]] = scope_counts["regional"].get(ref["census_region"], 0) + 1
            scope_counts["divisional"][ref["census_division"]] = scope_counts["divisional"].get(ref["census_division"], 0) + 1

    # ---------------------------------------------------------------------------
    # Build output records
    # ---------------------------------------------------------------------------

    # --- wordsmith JSON (latest month, one entry per state) ---
    wordsmith_entries: list[dict] = []
    for code in sorted(latest_states.keys()):
        ref = states_module.get_state_by_code(code)
        if ref is None:
            continue
        name = ref["name"]
        rate = latest_states[code]
        change, trend_val = mom_data.get((code, latest_date), (None, None))

        flags = list(qa_flags_map.get((code, latest_data_month), []))

        summary = _render_summary(name, rate, latest_date, change, trend_val)
        para2 = _render_ranking_paragraph(
            name, latest_date, "highest unemployment rate",
            code, rate_ranks, ref, scope_counts,
        )
        para3: str | None = None
        if change is not None:
            para3 = _render_ranking_paragraph(
                name, latest_date, "largest month-over-month change",
                code, mom_ranks, ref, scope_counts,
            )

        wordsmith_entries.append({
            "state": name,
            "state_code": code,
            "month": latest_data_month,
            "unemployment_rate": rate,
            "mom_change_pp": change,
            "trend_direction": trend_val,
            "summary_sentence": summary,
            "paragraph_2": para2,
            "paragraph_3": para3,
            "qa_flags": flags,
            "updated_at": datetime.now().isoformat(),
        })

    # --- map CSV (latest month only) ---
    map_rows: list[dict] = []
    for code in sorted(latest_states.keys()):
        ref = states_module.get_state_by_code(code)
        if ref is None:
            continue
        rate = latest_states[code]
        change, trend_val = mom_data.get((code, latest_date), (None, None))
        map_rows.append({
            "date": latest_date,
            "state_code": code,
            "state_name": ref["name"],
            "fips_code": ref["fips_code"],
            "unemployment_rate": f"{rate:.1f}",
            "mom_change_pp": f"{change:.1f}" if change is not None else "",
            "trend_direction": trend_val or "",
            "rate_rank_national": rate_ranks.get(code, {}).get("national", ""),
            "census_region": ref["census_region"],
            "census_division": ref["census_division"],
            "update_dttm": run_id,
        })

    # --- table CSV (latest month, sorted by national rate rank) ---
    table_rows: list[dict] = []
    for code in sorted(latest_states.keys(), key=lambda c: rate_ranks.get(c, {}).get("national", 999)):
        ref = states_module.get_state_by_code(code)
        if ref is None:
            continue
        rate = latest_states[code]
        change, trend_val = mom_data.get((code, latest_date), (None, None))
        # rr = rate_ranks.get(code, {})
        # mr = mom_ranks.get(code, {})
        table_rows.append({
            # "rank_national": rr.get("national", ""),
            "date": latest_date,
            "State": ref["name"],
            "state_code": code,
            "Unemployment Rate": f"{rate:.1f}",
            "Monthly Change": f"{change:.1f}" if change is not None else "",
            # "trend_direction": trend_val or "",
            # "rate_rank_region": rr.get("regional", ""),
            # "rate_rank_division": rr.get("divisional", ""),
            # "mom_rank_national": mr.get("national", ""),
            "Region": ref["census_region"],
            "Division": ref["census_division"],
            "update_dttm": run_id,
        })

    # ---------------------------------------------------------------------------
    # Write all output files
    # ---------------------------------------------------------------------------
    suffix = f"{latest_data_month}_{run_id}"

    _write_json(f"wordsmith_json_payload/wordsmith_{suffix}.json", wordsmith_entries)

    _write_csv(
        f"dw_viz_data/map_{suffix}.csv",
        map_rows,
        ["date", "state_code", "state_name", "fips_code", "unemployment_rate", "mom_change_pp",
         "trend_direction", "rate_rank_national", "census_region", "census_division", "update_dttm"],
    )
    _write_csv(
        f"dw_viz_data/table_{suffix}.csv",
        table_rows,
        ["date", "State", "state_code", "Unemployment Rate", "Monthly Change",
         "Region", "Division", "update_dttm"],
    )


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
    run_output(run_id=run_id)
    logger.info("output: done.")
