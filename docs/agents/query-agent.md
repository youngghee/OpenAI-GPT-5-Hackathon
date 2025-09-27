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
- Answer stakeholder questions using existing CRM data and lightweight analytics.
- Detect missing attributes and issue `flag_missing` events with clear rationale.
- Provide response provenance (table/column references, enriched facts consumed).

## Observability
- Log prompt, SQL snippets, and decision trace to `logs/query/<ticket>.jsonl` (planned).
- Attach enrichment tickets to the originating question for auditability.
