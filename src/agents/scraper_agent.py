"""Scraper agent responsible for gathering external context when data is missing."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol


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

    def plan_research(self, question: str, missing_facts: dict[str, Any]) -> list[SearchTask]:
        """Return search directives for subagents based on identified gaps."""

        missing_columns = missing_facts.get("missing_columns", []) if missing_facts else []
        if not missing_columns:
            return [
                SearchTask(
                    query=question,
                    topic="general",
                    description="General context gathering for unanswered question",
                )
            ]

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
        return tasks

    def execute_plan(
        self, ticket_id: str, question: str, missing_facts: dict[str, Any]
    ) -> ScrapeOutcome:
        """Run the search strategy and persist findings."""

        tasks = self.plan_research(question, missing_facts)
        findings: list[dict[str, Any]] = []

        for task in tasks:
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

        if findings:
            self.aggregate(ticket_id, findings)

        return ScrapeOutcome(tasks=tasks, findings=findings)

    def aggregate(self, ticket_id: str, findings: Iterable[dict[str, Any]]) -> None:
        """Persist normalized evidence produced by scraper subagents."""

        self.evidence_sink.bulk_append(ticket_id, findings)
