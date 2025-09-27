# Query Agent Profile

- **Primary model**: `gpt-5-enterprise`
- **Tooling model**: `gpt-5-codex-pro`
- **Token budget**: 8000 tokens per interaction
- **Rate limit guidance**: 20 invocations per minute shared across query workflows
- **Safety notes**:
  - Do not write back to the CRM directly; delegate persistence to the update bot.
  - Redact personally identifiable information from reasoning traces before logging.
  - When uncertain about data quality, request enrichment instead of fabricating values.

## Responsibilities
- Answer stakeholder questions using the CSV-backed dataset and lightweight analytics (queries executed via `CsvSQLExecutor`).
- Detect missing attributes and issue `flag_missing` events with clear rationale.
- Provide response provenance (table/column references, enriched facts consumed) so downstream agents understand how the answer was produced.

## Observability
- Log prompt, SQL snippets, and decision trace to `logs/query/<ticket>.jsonl` (planned).
- Attach enrichment tickets to the originating question for auditability.
- Capture derived SQL statements (e.g., `SELECT * FROM dataset WHERE BRIZO_ID = ...`) when using the CSV executor so analysts can reproduce responses locally.
