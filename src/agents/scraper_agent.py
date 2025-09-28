"""Scraper agent responsible for gathering external context when data is missing."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlparse

from src.core.observability import ScraperObservationSink
from src.integrations.openai_agent_sdk import OpenAIAgentAdapter


class SearchClient(Protocol):
    """Interface for running web searches or API lookups."""

    def search(
        self, query: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:  # pragma: no cover - interface
        """Return structured search results for a query."""


class EvidenceSink(Protocol):
    """Destinations that persist gathered evidence alongside provenance."""

    def append(
        self, ticket_id: str, payload: dict[str, Any]
    ) -> None:  # pragma: no cover - interface
        ...

    def bulk_append(
        self, ticket_id: str, payloads: Iterable[dict[str, Any]]
    ) -> None:  # pragma: no cover - interface
        ...


@dataclass(slots=True)
class SearchTask:
    """Represents a discrete search directive for a missing attribute."""

    query: str
    topic: str
    description: str

    def to_dict(self) -> dict[str, str]:
        return {"query": self.query, "topic": self.topic, "description": self.description}


@dataclass(slots=True)
class ScrapeOutcome:
    """Holds the outcome of executing a scraper plan."""

    tasks: list[SearchTask]
    findings: list[dict[str, Any]]
    successful_searches: list[dict[str, Any]] = field(default_factory=list)
    backfill_prompt: str | None = None


@dataclass
class ScraperAgent:
    """Drafts research plans, manages subagents, and collates findings."""

    search_client: SearchClient
    evidence_sink: EvidenceSink
    default_limit: int = 5
    logger: ScraperObservationSink | None = None
    llm_client: OpenAIAgentAdapter | None = None

    def plan_research(
        self,
        question: str,
        missing_facts: dict[str, Any],
        *,
        ticket_id: str | None = None,
    ) -> list[SearchTask]:
        """Return search directives for subagents based on identified gaps."""

        missing_columns = missing_facts.get("missing_columns", []) if missing_facts else []
        candidate_urls = (
            missing_facts.get("candidate_urls") if isinstance(missing_facts, dict) else None
        )
        record_context = (
            missing_facts.get("record_context") if isinstance(missing_facts, dict) else None
        )
        company_name = self._extract_company_name(record_context)

        tasks: list[SearchTask] = []
        if question:
            tasks.append(
                SearchTask(
                    query=self._compose_google_query(question, company_name),
                    topic="google",
                    description="General Google search scoped to the company",
                )
            )

        tasks.extend(self._candidate_url_tasks(question, candidate_urls))

        if self.llm_client:
            llm_tasks = self._plan_with_llm(
                question=question,
                missing_columns=missing_columns,
                record_context=record_context if isinstance(record_context, dict) else None,
                ticket_id=ticket_id,
            )
            if llm_tasks:
                tasks.extend(llm_tasks)

        if missing_columns:
            for column in missing_columns:
                readable = column.replace("_", " ").strip().lower()
                query_terms: list[str] = []
                if company_name:
                    query_terms.append(f'"{company_name}"')
                if question:
                    query_terms.append(question)
                if readable:
                    query_terms.append(readable)
                query = " ".join(filter(None, query_terms)) or question
                tasks.append(
                    SearchTask(
                        query=query,
                        topic=column,
                        description=f"Find supporting evidence for missing column '{column}'",
                    )
                )
        elif not candidate_urls:
            # If we have no specific missing columns or URLs, add a general follow-up search.
            tasks.append(
                SearchTask(
                    query=(f'"{company_name}" {question}' if company_name else question)
                    or "company background",
                    topic="general",
                    description="General context gathering for unanswered question",
                )
            )

        if company_name:
            self._inject_company_name(tasks, company_name)

        self._log_event(
            ticket_id=ticket_id,
            event="scrape_plan_created",
            payload={
                "question": question,
                "task_count": len(tasks),
                "missing_columns": missing_columns,
            },
        )
        return tasks

    def execute_plan(
        self, ticket_id: str, question: str, missing_facts: dict[str, Any]
    ) -> ScrapeOutcome:
        """Run the search strategy and persist findings."""

        tasks = self.plan_research(question, missing_facts, ticket_id=ticket_id)
        findings: list[dict[str, Any]] = []
        successful: list[dict[str, Any]] = []

        for task in tasks:
            self._log_event(
                ticket_id=ticket_id,
                event="scrape_task_started",
                payload={"topic": task.topic, "query": task.query},
            )
            results = self.search_client.search(task.query, limit=self.default_limit)
            for rank, result in enumerate(results):
                findings.append(
                    {
                        "ticket_id": ticket_id,
                        "topic": task.topic,
                        "query": task.query,
                        "rank": rank,
                        "result": result,
                    }
                )
            if results:
                successful.append(
                    {
                        "topic": task.topic,
                        "query": task.query,
                        "description": task.description,
                        "result_count": len(results),
                    }
                )
            self._log_event(
                ticket_id=ticket_id,
                event="scrape_task_completed",
                payload={
                    "topic": task.topic,
                    "query": task.query,
                    "result_count": len(results),
                },
            )

        if findings:
            self.aggregate(ticket_id, findings)
        else:
            self._log_event(
                ticket_id=ticket_id,
                event="scrape_no_findings",
                payload={"question": question, "task_count": len(tasks)},
            )
        missing_columns: list[str] = []
        if isinstance(missing_facts, dict):
            raw_missing = missing_facts.get("missing_columns")
            if isinstance(raw_missing, list):
                missing_columns = [str(item) for item in raw_missing if item]

        backfill_prompt = self._compose_backfill_prompt(
            question=question,
            missing_columns=missing_columns,
            successful=successful,
        )

        return ScrapeOutcome(
            tasks=tasks,
            findings=findings,
            successful_searches=successful,
            backfill_prompt=backfill_prompt,
        )

    def aggregate(self, ticket_id: str, findings: Sequence[dict[str, Any]]) -> None:
        """Persist normalized evidence produced by scraper subagents."""

        self.evidence_sink.bulk_append(ticket_id, findings)
        self._log_event(
            ticket_id=ticket_id,
            event="scrape_findings_persisted",
            payload={"count": len(findings)},
        )

    def _log_event(self, ticket_id: str | None, event: str, payload: dict[str, Any]) -> None:
        if not ticket_id or self.logger is None:
            return
        try:
            self.logger.log_event(ticket_id, event, payload)
        except Exception:
            # Observability must never block scraping.
            pass

    def _plan_with_llm(
        self,
        *,
        question: str,
        missing_columns: list[str],
        record_context: dict[str, Any] | None,
        ticket_id: str | None,
    ) -> list[SearchTask]:
        if self.llm_client is None:
            return []
        column_text = ", ".join(missing_columns) if missing_columns else "none"
        context_lines = self._format_record_context(record_context)
        messages = [
            {
                "role": "system",
                "content": (
                    "You assist a scraper agent. Given a business question and missing"
                    " attributes, propose specific web searches. Return up to 5 lines"
                    " in the format 'topic | query | description'."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Question: {question}\nMissing columns: {column_text}\n"
                    f"Company context: {context_lines}\n"
                    "Generate focused search directives."
                ),
            },
        ]
        try:
            response = self.llm_client.generate(messages=messages)
        except Exception as exc:  # pragma: no cover - defensive fallback
            self._log_event(
                ticket_id,
                "llm_error",
                {"error": str(exc)},
            )
            return []

        tasks = self._parse_llm_plan(response)
        if tasks:
            self._log_event(
                ticket_id,
                "llm_plan_created",
                {"task_count": len(tasks)},
            )
        return tasks

    def _candidate_url_tasks(
        self, question: str, candidate_urls: Iterable[str] | None
    ) -> list[SearchTask]:
        if not candidate_urls:
            return []
        tasks: list[SearchTask] = []
        seen_hosts: set[str] = set()
        for url in candidate_urls:
            host = self._extract_host(url)
            if not host or any(keyword in host for keyword in self.IGNORED_HOST_KEYWORDS):
                continue
            if host in seen_hosts:
                continue
            seen_hosts.add(host)
            tasks.append(
                SearchTask(
                    query=f"site:{host} {question}",
                    topic=host,
                    description=f"Search {host} for information related to the question",
                )
            )
        return tasks

    @staticmethod
    def _extract_host(url: str) -> str | None:
        parsed = urlparse(url.strip())
        host = parsed.netloc or parsed.path.split("/")[0]
        return host.lower() if host else None

    IGNORED_HOST_KEYWORDS = {
        "foodmetrics",
        "internal",
        "example.com",
        "google.com",
        "g.page",
        "goo.gl",
    }

    @staticmethod
    def _extract_company_name(record_context: dict[str, Any] | None) -> str | None:
        if not record_context:
            return None
        preferred_keys = (
            "BUSINESS_NAME",
            "ALTERNATE_NAME",
            "CHAIN_NAME",
            "PARENT_NAME",
        )
        for key in preferred_keys:
            value = record_context.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if value:
                return str(value)
        return None

    @staticmethod
    def _compose_google_query(question: str, company_name: str | None) -> str:
        question = question.strip()
        if company_name:
            base = f'"{company_name}"'
            return f"{base} {question}" if question else base
        return f'"{question}"' if question else question

    @staticmethod
    def _format_record_context(record_context: dict[str, Any] | None) -> str:
        if not record_context:
            return "none"
        parts = []
        for key, value in record_context.items():
            if value is None:
                continue
            parts.append(f"{key}: {value}")
        return "; ".join(parts) if parts else "none"

    @staticmethod
    def _inject_company_name(tasks: list[SearchTask], company_name: str) -> None:
        replacements = {
            "{company}": company_name,
            "{Company}": company_name,
            "{company name}": company_name,
            "{Company Name}": company_name,
            "{business}": company_name,
            "{Business}": company_name,
        }

        for task in tasks:
            task.query = ScraperAgent._replace_placeholders(task.query, replacements)
            task.description = ScraperAgent._replace_placeholders(task.description, replacements)
            task.topic = ScraperAgent._replace_placeholders(task.topic, replacements)

    @staticmethod
    def _parse_llm_plan(response: Any) -> list[SearchTask]:  # pragma: no cover - exercised via tests
        lines: list[str] = []
        output = getattr(response, "output", [])
        for item in output:
            content = getattr(item, "content", [])
            if isinstance(content, list):
                for block in content:
                    text = getattr(block, "text", "")
                    if text:
                        lines.extend(line.strip() for line in text.splitlines() if line.strip())
            else:
                text = getattr(content, "text", "")
                if text:
                    lines.extend(line.strip() for line in text.splitlines() if line.strip())

        tasks: list[SearchTask] = []
        for raw in lines:
            parts = [part.strip() for part in raw.split("|")]
            if len(parts) < 2:
                continue
            topic = parts[0] or "general"
            query = parts[1] or ""
            description = parts[2] if len(parts) > 2 else ""
            if not query:
                continue
            tasks.append(SearchTask(topic=topic, query=query, description=description))
        return tasks

    @staticmethod
    def _replace_placeholders(value: str, replacements: dict[str, str]) -> str:
        if not value:
            return value
        result = value
        for placeholder, actual in replacements.items():
            result = result.replace(placeholder, actual)
        return result

    @staticmethod
    def _compose_backfill_prompt(
        *,
        question: str,
        missing_columns: list[str],
        successful: list[dict[str, Any]],
    ) -> str | None:
        if not successful:
            return None

        normalized_columns = [
            column.strip()
            for column in missing_columns
            if isinstance(column, str) and column.strip()
        ]
        if normalized_columns:
            focus_text = ", ".join(sorted({col for col in normalized_columns}))
            target_clause = f"to capture {focus_text}"
        else:
            target_clause = "to capture the missing information"

        question_text = question.strip() if question and question.strip() else "the stakeholder's request"

        lines = [
            "You are backfilling newly accepted schema fields across the dataset.",
            f"Reuse the proven searches below {target_clause} when answering '{question_text}'.",
            "For each remaining record:",
            "1. Run the recommended searches, adjusting company/location terms as needed.",
            "2. Pull values from authoritative sources returned by the searches.",
            "3. Capture the source URL for auditing and note any confidence caveats.",
            "Successful searches:",
        ]

        for index, entry in enumerate(successful, start=1):
            if not isinstance(entry, dict):
                continue
            topic = str(entry.get("topic", "")).strip()
            query = str(entry.get("query", "")).strip()
            description = str(entry.get("description", "")).strip()
            result_count = entry.get("result_count")
            summary = f"[{index}]"
            if topic:
                summary += f" Topic: {topic}"
            if query:
                summary += f" | Query: {query}"
            if description:
                summary += f" | Use: {description}"
            if isinstance(result_count, int):
                summary += f" (returned {result_count} result{'s' if result_count != 1 else ''})"
            lines.append(summary)

        lines.append("Document findings in the evidence log before importing into the CRM.")
        return "\n".join(lines)
