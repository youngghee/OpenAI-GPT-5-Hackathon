"""Scraper agent responsible for gathering external context when data is missing."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
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

        tasks: list[SearchTask] = []
        if question:
            tasks.append(
                SearchTask(
                    query=f'"{question}"',
                    topic="google",
                    description="General Google search for the question",
                )
            )

        tasks.extend(self._candidate_url_tasks(question, candidate_urls))

        if self.llm_client:
            llm_tasks = self._plan_with_llm(
                question=question,
                missing_columns=missing_columns,
                ticket_id=ticket_id,
            )
            if llm_tasks:
                tasks.extend(llm_tasks)

        if missing_columns:
            for column in missing_columns:
                readable = column.replace("_", " ").strip().lower()
                query = f"{question} {readable}" if readable else question
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
                    query=question or "company background",
                    topic="general",
                    description="General context gathering for unanswered question",
                )
            )

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

        return ScrapeOutcome(tasks=tasks, findings=findings)

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
        ticket_id: str | None,
    ) -> list[SearchTask]:
        if self.llm_client is None:
            return []
        column_text = ", ".join(missing_columns) if missing_columns else "none"
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
