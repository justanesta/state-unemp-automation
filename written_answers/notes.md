# Assumptions

- That the more recently published unemployment data for a given state/month should be given preference because of potential revisions or error corrections implemented since previous month's data. So the `unemployment_rate_prev_month` data Rhode Island for the 2025-12 row is given priority over the `unemployment_rate` data for Rhode Island in 2025-11.
- Every non-zero month-over-month change is reported with its direction and magnitude. The template renders all three cases ("up", "down", "flat") identically in structure. Rankings within division, region, and nationwide were added to convey additional relative magnitude of the change.
- "Flat" is only used when a change in the unemployment rate is exactly 0.0 percentage points between months.
- Dates are changed to the ISO 8601 YYYY-MM-DD format with month dates always defaulting to the first of the month (e.g. `2025-12-01` for December 2025).
- New data will come in the same file format (Excel) and shape (Month-over-month change wide). Ideally in future runs the excel file name structure would be in `snake_case` with more date and contextual measurement information like `us_state_unemp_2025-12.xlsx` so that raw data files could be auto-discovered in the `run_validation()` function in `validate.py` and checked against the last pipeline run state data in the `.pipeline_state/` folder.
- The `RATE_WARNING_THRESHOLD` constant in `validate.py` is set to warn at unemployment rates at 15% (eye-balling based on historical unemployment data). This can be editing or expanded upon.
- The `PUBLISH_GATE_THRESHOLD` constant in `validate.py` is set to throw an error if >40% of states are not publishable for reasons outlined in `data_cleaning_qa_skip_logic.md`. This can be edited.

***Important information to ascertain to build a stronger, longer lasting pipeline***:
1. Does new data have to come in the Excel format?
   1. If so, how variable will the filename be? The sheetname? The column fields? The column names?
2. Will data always be in the wide format with the current data month as well as the previous data month?
   1. If so, which data figure (any given data month' `unemployment_rate` value or the subsequent month's `unemployment_rate_prev_month` value) is more reliable/useful?
3. Is there a data dictionary or reference material for the source of this data? Can that be programmatically integrated to sync with data validation modeling?

# Potential Future Improvement

- Include `state_fips` in `clean_data/clean_data.jsonl` and sort by state and date.
- Include year-over-year changes in the `_render_summary()` templates in `output.py` and include historical data for each state so information like "This is the largest {month-over-month} {increase/decline} in the {unemployment rate} in {Kansas} since {April 2020} could be added to the wordsmith payload.
- Shared utitlies like `states.py` can be moved into a shared utility folder like `utils/` and have functions and classes imported from other script from there.
- If relaxing the `not_publishable` designation based on non-matching month-over-month changes from new data is desired, log a warning to the `qa_flag` that notes if a change (due to a revision, error correction, etc.) is more than a configured threshold.
- Switch to pandas, polars, or parquet for data processing as necessary with size scaling. Potentially even partioning data storage for `clean_data/` by some field if data gets really big (not as relevant for this example).
- Implementing schema versioning for `CleanRow` with pydantic if/as data columns change/are added.
- Async or DAG-based step execution with Airflow or other python-friendly orchestrator instead of batched sequential scripts.
- Implementing the [Datawrapper API](https://developer.datawrapper.de/reference/introduction) directly or via the [datawrapper](https://pypi.org/project/datawrapper/) python library to automatically update visualizations based on successful script execution.
