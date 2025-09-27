"""Tests for the scraper agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.agents.scraper_agent import ScraperAgent, SearchClient
from src.core.evidence import EvidenceSink, JSONLEvidenceSink


@dataclass
class _SearchClientStub(SearchClient):
    responses: dict[str, list[dict[str, Any]]]
    queries: list[str] = field(default_factory=list)

    def search(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:  # type: ignore[override]
        self.queries.append(query)
        return self.responses.get(query, [])[: limit or None]


@dataclass
class _SinkStub(EvidenceSink):
    appended: list[dict[str, Any]] = field(default_factory=list)

    def append(self, ticket_id: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        self.appended.append({"ticket_id": ticket_id, **payload})

    def bulk_append(self, ticket_id: str, payloads: Any) -> None:  # type: ignore[override]
        for payload in payloads:
            self.append(ticket_id, payload)


def test_plan_research_for_missing_columns() -> None:
    agent = ScraperAgent(
        search_client=_SearchClientStub(responses={}),
        evidence_sink=_SinkStub(),
    )

    tasks = agent.plan_research(
        question="What is the business name?",
        missing_facts={"missing_columns": ["BUSINESS_NAME", "LOCATION_CITY"]},
    )

    expected_task_count = 2
    assert len(tasks) == expected_task_count
    assert tasks[0].topic == "BUSINESS_NAME"
    assert "business name" in tasks[0].query.lower()


def test_execute_plan_persists_findings(tmp_path: Path) -> None:
    query = "What is the business name? business name"
    search_client = _SearchClientStub(
        responses={query: [{"url": "https://example.com", "title": "Example"}]}
    )
    sink = JSONLEvidenceSink(base_dir=tmp_path)
    agent = ScraperAgent(search_client=search_client, evidence_sink=sink)

    outcome = agent.execute_plan(
        ticket_id="T-1",
        question="What is the business name?",
        missing_facts={"missing_columns": ["BUSINESS_NAME"]},
    )

    assert outcome.findings
    evidence_file = tmp_path / "T-1.jsonl"
    assert evidence_file.exists()
    content = evidence_file.read_text(encoding="utf-8").strip()
    assert "https://example.com" in content
