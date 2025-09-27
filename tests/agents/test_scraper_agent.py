"""Tests for the scraper agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.agents.scraper_agent import ScraperAgent, SearchClient
from src.core.evidence import EvidenceSink, JSONLEvidenceSink
from src.core.observability import ScraperObservationSink


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


@dataclass
class _ScraperLoggerStub(ScraperObservationSink):
    events: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        self.events.append((ticket_id, event, payload))


class _LLMStub:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[dict[str, Any]] = []

    def generate(self, *, messages: list[dict[str, str]], max_output_tokens: int | None = None, tools=None) -> Any:  # type: ignore[override]
        self.calls.append({"messages": messages})

        class _Response:
            def __init__(self, payload: str) -> None:
                self.output = [
                    type(
                        "Block",
                        (),
                        {
                            "content": [type("Text", (), {"text": payload})()],
                        },
                    )()
                ]

        return _Response(self.text)


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
    logger = _ScraperLoggerStub()
    agent = ScraperAgent(search_client=search_client, evidence_sink=sink, logger=logger)

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
    events = [event for _, event, _ in logger.events]
    assert "scrape_plan_created" in events
    assert "scrape_task_started" in events
    assert "scrape_task_completed" in events
    assert "scrape_findings_persisted" in events


def test_execute_plan_logs_when_no_findings() -> None:
    search_client = _SearchClientStub(responses={})
    logger = _ScraperLoggerStub()
    agent = ScraperAgent(
        search_client=search_client,
        evidence_sink=_SinkStub(),
        logger=logger,
    )

    outcome = agent.execute_plan(
        ticket_id="T-2",
        question="What is the business name?",
        missing_facts={},
    )

    assert not outcome.findings
    events = [event for _, event, _ in logger.events]
    assert "scrape_no_findings" in events


def test_llm_plan_creates_tasks() -> None:
    llm = _LLMStub("BUSINESS_NAME|what is the business name? | Look for official site")
    logger = _ScraperLoggerStub()
    agent = ScraperAgent(
        search_client=_SearchClientStub(responses={}),
        evidence_sink=_SinkStub(),
        logger=logger,
        llm_client=llm,
    )

    tasks = agent.plan_research(
        question="What is the business name?",
        missing_facts={"missing_columns": ["BUSINESS_NAME"]},
        ticket_id="T-LLM",
    )

    assert tasks and tasks[0].topic == "BUSINESS_NAME"
    assert llm.calls
