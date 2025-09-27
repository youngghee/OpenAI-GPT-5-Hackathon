# CSV-Based Data Access

For local development and early-stage simulations, the platform operates entirely
against CSV exports instead of a live relational database. The CSV executor
(`src/integrations/csv_sql_executor.py`) loads the dataset into memory and
supports simple `SELECT ... WHERE column = 'value'` queries, allowing the query
agent to behave as if it were connected to a warehouse.

## CSV Executor Capabilities
- Loads header metadata and normalizes column lookups.
- Supports equality filters plus optional `LIMIT` clauses.
- Returns dictionaries keyed by CSV column names, preserving casing.
- Companion inspector (`python -m src.integrations.csv_dataset /path/to/export.csv`)
  reports column lists, row counts, and sample rows for quick sanity checks.

## Operational Caveats
- Complex SQL (joins, aggregates, nested queries) is not supported in the CSV
  executor. If richer analytics are needed, translate the logic into Python or
  precompute derived fields within the CSV.
- The CSV file path is configured via `CSV_DATA_PATH` in `.env` and referenced
  through `configs/dev.yaml` (`data_sources.csv`).
- Because the data resides entirely in memory, large exports may impact memory
  usage. Consider filtering or sampling the dataset for development scenarios.

## Extending Support
When the system transitions to a live database, replace
`CsvSQLExecutor` with an adapter that implements `SQLExecutor`. Existing agents
(`QueryAgent`, `ScraperAgent`, etc.) will continue to function without code
changes as long as the interface contract is honored.
