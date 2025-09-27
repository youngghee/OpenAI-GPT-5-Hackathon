# Scraper Agent Profile

- **Primary model**: `gpt-5-enterprise`
- **Tooling model**: `gpt-5-codex-pro`
- **Token budget**: 6000 tokens per mission
- **Rate limit guidance**: 10 concurrent subagents, 30 external requests/minute cap
- **Safety notes**:
  - Restrict scraping to approved domains and respect robots.txt guidelines.
  - Record full source URLs and timestamps for each captured fact.
  - Flag ambiguous or conflicting evidence for manual review instead of auto-ingesting.

## Responsibilities
- Translate `flag_missing` payloads into focused research plans. When configured with the GPT-5 Responses model (`response_model_id`), the agent asks the LLM—via the OpenAI Agents SDK when available—to draft targeted search directives before falling back to rule-based heuristics.
- Seed each plan with a general Google query and `site:` searches derived from record URLs (link, Yelp, delivery platforms) while skipping internal domains.
- Coordinate optional subagents to gather and normalize evidence.
- Call the OpenAI Responses API web-search tool when configured (`search.provider: openai`)
  to retrieve canonical snippets and metadata.
- Persist findings to `assets/scrapes/<ticket>.jsonl` with provenance metadata via the shared JSONL evidence sink.
- Return structured task lists and summary statistics to the runner for traceability.

## Observability
- Emit JSONL progress events (`scrape_plan_created`, `scrape_task_started`, `llm_plan_created`, etc.) to `logs/scraper/<ticket>.jsonl`.
- Track external API usage to enforce rate limits and cost controls. Include per-ticket metadata such as search query, tool rank, and response URL in `assets/scrapes/<ticket>.jsonl` for auditability.
