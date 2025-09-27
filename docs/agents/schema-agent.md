# Schema Agent Profile

- **Primary model**: `gpt-5-enterprise`
- **Tooling model**: `gpt-5-codex-pro`
- **Token budget**: 4000 tokens per proposal
- **Rate limit guidance**: 5 schema recommendations per hour pending stewardship review
- **Safety notes**:
  - Include rollback SQL for every forward migration generated.
  - Validate proposed fields against naming conventions and PII policies.
  - Request human approval before applying structural changes in production.

## Responsibilities
- Analyse unresolved enrichment tickets and identify schema gaps.
- Propose new columns with inferred data types, nullable defaults, and documentation context. When GPT-5 (`response_model_id`) is available, proposals are sourced from the Agents SDK response before falling back to deterministic inference.
- Emit timestamped SQL migrations to `schema/migrations/` via the shared migration writer.

## Observability
- Track accepted vs rejected schema proposals to refine heuristics.
- Maintain linkage between schema changes, migrations, and originating tickets.
