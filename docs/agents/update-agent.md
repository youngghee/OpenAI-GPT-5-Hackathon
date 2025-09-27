# Update Agent Profile

- **Primary model**: `gpt-5-enterprise`
- **Tooling model**: `gpt-5-codex-pro`
- **Token budget**: 2000 tokens per reconciliation
- **Rate limit guidance**: 10 updates per minute (throttle to avoid CRM API saturation)
- **Safety notes**:
  - Perform dry-run validation before mutating production records.
  - Require explicit user approval when confidence in enrichment < 0.7.
  - Escalate anomalies instead of overwriting conflicting values.

## Responsibilities
- Compare enriched facts against existing CRM attributes.
- Apply deterministic field updates and log rationale for each change (logged via the in-memory CRM client in development or the production integration). When configured with GPT-5 (`response_model_id`), the agent generates a natural-language explanation of applied and escalated fields using the Agents SDK fallback pipeline.
- Escalate schema gaps with supporting context for downstream review.
- Emit structured JSONL records under `schema/escalations/` summarising unknown or empty fields that blocked an update.

## Observability
- Produce structured audit logs for each record update.
- Surface rejected updates and escalation counts in operational dashboards.
- Include the GPT-authored reasoning snippet in chat transcripts and logs when available.
