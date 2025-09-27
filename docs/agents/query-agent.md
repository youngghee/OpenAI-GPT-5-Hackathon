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
- Answer stakeholder questions using the CSV-backed dataset and lightweight analytics (queries executed via `CsvSQLExecutor`). When direct column matches are absent, the agent consults the GPT-5 Responses model via the OpenAI Agents SDK (configurable via `response_model_id`, with automatic Responses API fallback) to synthesize a JSON object of column/value pairs constrained to the provided row context.
- Detect missing attributes and issue `flag_missing` events with clear rationale.
- Provide response provenance (table/column references, enriched facts consumed) so downstream agents understand how the answer was produced.

## Observability
- Emits lifecycle events (`question_received`, `sql_executed`, `columns_inferred`, `llm_answer`, etc.) to `logs/query/<ticket>.jsonl` for each ticket using the JSONL query logger, including the LLM column selections.
- Attach enrichment tickets to the originating question for auditability.
- Capture derived SQL statements (e.g., `SELECT * FROM dataset WHERE BRIZO_ID = ...`) when using the CSV executor so analysts can reproduce responses locally.
