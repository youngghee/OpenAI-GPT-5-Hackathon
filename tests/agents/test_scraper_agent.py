"""Tests for the scraper agent."""

from __future__ import annotations

import json
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

    assert len(tasks) == 3
    assert tasks[0].topic == "google"
    column_topics = {task.topic for task in tasks[1:]}
    assert "BUSINESS_NAME" in column_topics
    assert any("business name" in task.query.lower() for task in tasks)


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
    assert outcome.successful_searches
    first_success = outcome.successful_searches[0]
    assert first_success["query"] == query
    assert first_success["topic"] == "BUSINESS_NAME"
    assert outcome.backfill_prompt
    assert "business name" in outcome.backfill_prompt.lower()
    files = sorted(tmp_path.glob("*-T-1.jsonl"))
    assert len(files) == 1
    evidence_file = files[0]
    lines = evidence_file.read_text(encoding="utf-8").splitlines()
    assert lines
    first_entry = json.loads(lines[0])
    assert first_entry["result"]["url"] == "https://example.com"
    assert "timestamp" in first_entry
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
    assert outcome.successful_searches == []
    assert outcome.backfill_prompt is None
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

    assert tasks and tasks[0].topic == "google"
    assert llm.calls


def test_candidate_urls_influence_tasks() -> None:
    agent = ScraperAgent(
        search_client=_SearchClientStub(responses={}),
        evidence_sink=_SinkStub(),
    )

    tasks = agent.plan_research(
        question="Check delivery options",
        missing_facts={
            "candidate_urls": [
                "https://www.pigglywiggly.com/menu",
                "https://facebook.com/piggly",
                "https://internal.example.com/notes",
            ]
        },
    )

    topics = {task.topic for task in tasks}
    assert "google" in topics
    assert any("pigglywiggly.com" in topic for topic in topics)
    assert any("facebook.com" in topic for topic in topics)


def test_company_context_injected_into_queries() -> None:
    agent = ScraperAgent(
        search_client=_SearchClientStub(responses={}),
        evidence_sink=_SinkStub(),
    )

    tasks = agent.plan_research(
        question="employee count",
        missing_facts={"record_context": {"BUSINESS_NAME": "Piggly Wiggly"}},
    )

    google_task = next(task for task in tasks if task.topic == "google")
    assert '"Piggly Wiggly"' in google_task.query
    assert all("{Company" not in task.query for task in tasks)


def test_llm_placeholders_replaced_with_company() -> None:
    llm = _LLMStub(
        "LinkedIn company page|site:linkedin.com/company \"{Company Name}\"|Check LinkedIn headcount"
    )
    agent = ScraperAgent(
        search_client=_SearchClientStub(responses={}),
        evidence_sink=_SinkStub(),
        llm_client=llm,
    )

    tasks = agent.plan_research(
        question="employee count",
        missing_facts={"record_context": {"BUSINESS_NAME": "Piggly Wiggly"}},
        ticket_id="T-ctx",
    )

    assert any("Piggly Wiggly" in task.query for task in tasks if "linkedin" in task.query.lower())
    assert llm.calls
