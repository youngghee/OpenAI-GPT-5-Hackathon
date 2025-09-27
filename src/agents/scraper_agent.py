"""Scraper agent responsible for gathering external context when data is missing."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from src.core.observability import ScraperObservationSink


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

    def plan_research(
        self, question: str, missing_facts: dict[str, Any], *, ticket_id: str | None = None
    ) -> list[SearchTask]:
        """Return search directives for subagents based on identified gaps."""

        missing_columns = missing_facts.get("missing_columns", []) if missing_facts else []
        if not missing_columns:
            tasks = [
                SearchTask(
                    query=question,
                    topic="general",
                    description="General context gathering for unanswered question",
                )
            ]
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

        tasks: list[SearchTask] = []
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
