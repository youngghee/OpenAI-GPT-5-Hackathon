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
- Propose new columns with data types, constraints, and documentation updates.
- Generate Codex-authored migrations stored under `schema/` for later execution.

## Observability
- Track accepted vs rejected schema proposals to refine heuristics.
- Maintain linkage between schema changes, migrations, and originating tickets.
