# Architecture

The pipeline is a three-step, run-sequentially process: **validate → clean → output**. The orchestrator in `main.py` calls each step in order, threads a `run_id` (timestamp) through every stage, and writes a manifest after each transition so the run state is always recoverable from disk.

```
raw_data/*.xlsx
      │
      ▼
┌─────────────┐   validated rows   ┌───────────┐   clean JSONL   ┌────────────┐
│  validate   │ ─────────────────► │   clean   │ ───────────────►│   output   │
│  (Step 1)   │                    │  (Step 2) │                 │  (Step 3)  │
└──────┬──────┘                    └─────┬─────┘                 └──────┬─────┘
  write│                           write │                         read  │
       ▼                                 ▼                               ▼
       └─────────────────────────────────┴───── .pipeline_state/ ───────┘
                                    (run manifests & QA metadata)

Per-step file output:
  validated_data/                         ← Step 1
  clean_data/clean_data.jsonl             ← Step 2  (append-only)
  wordsmith_json_payload/ & dw_viz_data/  ← Step 3
```

---

## Step 1 — Validate (`validate.py`)

**Input:** the single xlsx in `raw_data/` (sheet `in`).
**Output:** `ValidatedRow` objects (passed in-memory to Step 2), `validated_data/`, and a manifest in `.pipeline_state/validate_output.json`.

This step does everything between raw ingest and the go/no-go decision to continue.

1. **Structural parse.** Each xlsx row is loaded into a `RawRow` pydantic model. Rows that fail structural validation (wrong types, missing required fields) are dropped immediately.

2. **State resolution.** The `state_code` column is authoritative — the `state` name column is informational only. Each code is looked up against the 50-state reference in `states.py`. Unknown codes make a row unpublishable; name mismatches are flagged but do not block.

3. **Date normalization.** Both `YYYY-MM` and `YYYY/MM` are accepted; everything is normalized to `YYYY-MM`. Unparseable dates block the row.

4. **Rate plausibility.** Two tiers:
   - **Hard block** — rate is `None`, negative, or >= 100. The row is unpublishable.
   - **Warning only** — rate >= 15.0. Flagged as `rate_unusually_high`; the row stays publishable. (The Rhode Island 27.0 value falls here and is later corrected by the pivot in Step 2.)

5. **Cross-row conflict check.** If a later month's `prev_month` value disagrees with an earlier month's reported current rate for the same state, the earlier row is blocked. This catches upstream revisions before they reach the clean step.

6. **Publish gate.** The number of states that are *fully* unpublishable (every single row for that state is blocked) is compared against a 40 % threshold of 50 states. If the gate trips, the pipeline writes an `ABORTED` manifest and exits non-zero — no output files are produced.

All QA flags accumulated during validation travel with the data and surface in the final wordsmith JSON payload.

---

## Step 2 — Clean (`clean.py`)

**Input:** the `ValidatedRow` list from Step 1.
**Output:** rows appended to `clean_data/clean_data.jsonl`; manifest written to `.pipeline_state/clean_output.json`.

The source xlsx is in **wide** format: each row carries both the current month's rate and the previous month's rate in separate columns. This step reshapes it into **long** format (one row per state per month) and resolves any data revisions in the process.

1. **Filter.** Only `is_publishable == True` rows proceed.

2. **Deduplicate.** Exact duplicate rows (same state, month, current rate, and prev-month rate) are collapsed. The source dataset contains five such duplicates at the end of the xlsx.

3. **Pivot with sorted last-write-wins.** Rows are sorted by `month_canonical` ascending, then each wide row is expanded into 1–2 long rows: one for the current month, one for the previous month (if `prev_month` is non-null). When two expansions produce conflicting values for the same `(state_code, date)` key, the one from the *later* source month wins. This is the mechanism that corrects revisions — for example, Rhode Island's November rate was reported as 27.0 in the November row, but December's `prev_month` correctly stated 5.3; because December is sorted after November, 5.3 overwrites 27.0.

4. **Append to JSONL.** Each `CleanRow` is written as a single JSON line with an `ingest_run` timestamp. The file is append-only; re-running the pipeline adds new lines rather than replacing old ones.

---

## Step 3 — Output (`output.py`)

**Input:** `clean_data/clean_data.jsonl` and `.pipeline_state/validate_output.json` (for QA flags).
**Output:** one wordsmith JSON file and three Datawrapper CSVs, all timestamped.

1. **Latest-version read.** The entire JSONL is scanned; for each `(state_code, date)` key, only the row with the highest `ingest_run` timestamp is kept. This makes the append-only store safe to re-run without manual cleanup.

2. **Month-over-month computation.** For every state with data in two consecutive months, the change is `round(current - prev, 1)` and classified as `up`, `down`, or `flat`. Rankings use the absolute value of the change (magnitude regardless of direction).

3. **Rankings.** Competition ranking (1, 1, 3 style — ties share a rank, the next rank skips) is computed at three geographic scopes: national, Census region (4), and Census division (9). Two independent ranking sets are produced: one for the unemployment rate, one for the MoM absolute change.

4. **Template rendering.** All user-facing copy is produced by deterministic string templates — no generative AI is involved. Each state gets up to three paragraphs:
   - A summary sentence: rate, AP-formatted date, and direction/magnitude of change (or "not available").
   - A rate-ranking paragraph: divisional, regional, and national rank with scope sizes.
   - A MoM-ranking paragraph: same structure, only emitted when MoM data exists.

   AP Style is enforced in code: `percent` not `%`, `percentage points` for changes, correct month abbreviations (March–July spelled out), possessive `'s` on all state names, and ordinals via a helper function.

5. **Output files.** All filenames include the latest data month and the run ID for traceability.
   - `wordsmith_*.json` — the per-state copy payload, ready to be injected into a CMS or page template.
   - `chart_*.csv` — all months, all states; intended for line or bar charts.
   - `map_*.csv` — latest month only, includes FIPS codes for choropleth joins.
   - `table_*.csv` — latest month, sorted by national rate rank, with all ranking columns.

---

## Key design decisions

| Decision | Rationale |
|---|---|
| Append-only JSONL with `ingest_run` timestamps | Re-running the pipeline is safe and idempotent. The output step always resolves to the latest version per key. |
| Sorted pivot for revision resolution | Data revisions arrive naturally as next-month `prev_month` values. Sorting by month and letting later writes win handles this without a separate reconciliation step. |
| `state_code` as the authoritative identifier | State names in source data are inconsistent (abbreviations, bare codes). The two-letter code is unambiguous and maps directly to the reference table and FIPS codes. |
| Publish gate checked against 50, not row count | The gate is a data-completeness signal. Checking against the fixed universe of 50 states catches cases where entire states are simply missing from the source, not just rows that fail validation. |
| All copy from templates, never generated | Templated output is deterministic, auditable, and version-controllable. It also makes AP Style enforcement a code concern rather than a prompt-engineering concern. |
