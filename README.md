# state-unemp-automation

A pipeline that ingests a monthly state unemployment xlsx, validates and cleans the data, computes rankings and month-over-month changes, and produces templated natural-language copy, Datawrapper-ready CSVs, and editorial QA artifacts — all without any generative AI in the output.

## Setup

Requires Python 3.14 and [uv](https://docs.astral.sh/uv/).

Install the dependencies 
```sh
uv sync
```

## Running the pipeline

Run the full validate → clean → output pipeline in one command:

```sh
uv run python main.py
```

Each step can also run standalone:

```sh
uv run python validate.py
uv run python clean.py
uv run python output.py
```

## Running tests

```sh
uv run pytest tests/ -v
```

## Output locations

| Directory | Contents |
|---|---|
| `wordsmith_json_payload/` | Per-state templated copy (JSON) |
| `dw_viz_data/` | Chart, map, and table CSVs for Datawrapper |
| `clean_data/` | Append-only long-format data store (JSONL) |
| `.pipeline_state/` | Per-run manifests and validation metadata |

## Assignment answers

Detailed answers to the assignment questions — including cleaning decisions, the troubleshooting scenario, sourcing notes, and the AP-style editorial pass — are in the [`written_answers/`](written_answers/) folder.

## Architecture

For a walkthrough of how the three pipeline steps fit together, see [ARCHITECTURE.md](ARCHITECTURE.md).
