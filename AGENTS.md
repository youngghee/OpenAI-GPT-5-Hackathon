# Repository Guidelines

## Architecture Overview
The query bot answers row-level questions with GPT-5, using Codex to run SQL when the built-in context is insufficient. When a `flag_missing` call fires, the scraper bot composes search plans, fans out optional subagents, and aggregates results in `assets/scrapes/<ticket>.jsonl`. After enrichment, the update bot reconciles responses with the CRM; if no field fits, it forwards the record, rationale, and evidence to the schema bot, which outputs column specs plus Codex-generated migrations. See the full multi-agent loop, diagram, and hand-off details in [`docs/self-enriching-business-intelligence.md`](docs/self-enriching-business-intelligence.md).

## Project Layout
Keep agents in `src/agents/` and orchestration kernels in `src/core/`. Connectors and third-party APIs live under `src/integrations/`, with prompt templates in `assets/prompts/`. Place database schemas and migration plans in `schema/`, configuration in `configs/{environment}.yaml`, and runtime narratives or ADRs in `docs/`. Tests must mirror package paths under `tests/`.

## Model & Codex Requirements
Default to GPT-5 Enterprise for reasoning and GPT-5 Codex Pro for tool execution; pin both in environment configs and expose overrides via `MODEL_ID` and `CODEX_ID`. Capture token budgets, rate limits, and safety instructions in each agent’s entry within `docs/agents/`.

## Development Workflow
Bootstrap with `python -m venv .venv` and `pip install -r requirements.txt`. Format via `black src tests` and lint with `ruff check src tests`. Run multi-agent simulations through `python -m src.core.runner --profile dev` and verify pipelines with `pytest --maxfail=1 --disable-warnings`. Use `make ci` before any pull request to chain lint, type, and test checks.

## Coding Standards
Target Python 3.11+, four-space indentation, exhaustive type hints, and docstrings on public entry points. Name agents by role (`query_agent.py`, `schema_agent.py`), keep functions under 40 lines, and extract branching logic into helper objects. Persist prompts as lowercase-hyphen Markdown files and version prompt changes alongside code updates.

## Testing Guidelines
Author `pytest` suites with fixture data in `tests/fixtures/` and name scenarios `test_<agent>_<case>.py`. Aim for ≥90% branch coverage, mark network-dependent tests with `@pytest.mark.external`, and gate them behind an opt-in flag in CI. Whenever the schema bot proposes a change, add a regression that replays the original question to confirm the gap is closed.

## Commit & Review Process
Follow Conventional Commits (`feat:`, `fix:`, `chore:`) and keep each commit scoped to one behaviour. Pull requests need a concise problem statement, checklist of affected agents, validation logs, and references to tickets or knowledge-base entries. Seek sign-off from the data stewardship group on schema proposals and attach generated migration scripts for review.
