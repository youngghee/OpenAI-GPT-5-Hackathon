"""Behavioural tests for the query agent."""

from __future__ import annotations

import json

from dataclasses import dataclass, field
from typing import Any

from src.agents.query_agent import MissingDataFlagger, QueryAgent, SQLExecutor
from src.core.observability import QueryObservationSink


@dataclass
class _SQLExecutorStub(SQLExecutor):
    rows: list[dict[str, Any]]
    statements: list[str] = field(default_factory=list)

    def run(self, statement: str) -> list[dict[str, Any]]:  # type: ignore[override]
        self.statements.append(statement)
        return self.rows


@dataclass
class _FlaggerStub(MissingDataFlagger):
    calls: list[dict[str, Any]] = field(default_factory=list)

    def flag_missing(  # type: ignore[override]
        self, ticket_id: str, question: str, facts: dict[str, Any]
    ) -> None:
        self.calls.append({"ticket_id": ticket_id, "question": question, "facts": facts})


@dataclass
class _LoggerStub(QueryObservationSink):
    events: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        self.events.append((ticket_id, event, payload))


def _make_agent(
    rows: list[dict[str, Any]],
    logger: QueryObservationSink | None = None,
    llm_client: Any | None = None,
    dataset_columns: list[str] | None = None,
) -> tuple[QueryAgent, _SQLExecutorStub, _FlaggerStub]:
    executor = _SQLExecutorStub(rows=rows)
    flagger = _FlaggerStub()
    return (
        QueryAgent(
            sql_executor=executor,
            missing_data_flagger=flagger,
            llm_client=llm_client,
            logger=logger,
            dataset_columns=dataset_columns,
        ),
        executor,
        flagger,
    )


def test_answer_question_returns_column_value() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "Cafe Example",
            "LOCATION_CITY": "Florence",
        }
    ]
    agent, executor, flagger = _make_agent(rows)

    result = agent.answer_question(
        ticket_id="T-1", question="What is the business name?", record_id="abc"
    )

    assert result["status"] == "answered"
    facts = result["facts"]
    assert isinstance(facts, list) and facts
    first_fact = facts[0]
    assert first_fact["concept"] == "business_name"
    assert first_fact["value"] == "Cafe Example"
    assert result["answer_origin"] == "dataset"
    assert executor.statements == ["SELECT * FROM dataset WHERE BRIZO_ID = 'abc' LIMIT 1"]
    assert flagger.calls == []


def test_answer_question_handles_missing_record() -> None:
    agent, _, flagger = _make_agent(rows=[])

    result = agent.answer_question(
        ticket_id="T-2", question="What is the business name?", record_id="missing"
    )

    assert result["status"] == "record_not_found"
    assert flagger.calls and flagger.calls[0]["facts"]["reason"] == "record_not_found"


def test_answer_question_flags_unknown_question() -> None:
    rows = [{"BRIZO_ID": "abc", "BUSINESS_NAME": "Cafe Example"}]
    agent, _, flagger = _make_agent(rows)

    result = agent.answer_question(
        ticket_id="T-3", question="What is their favorite color?", record_id="abc"
    )

    assert result["status"] == "unknown_question"
    assert flagger.calls and flagger.calls[0]["facts"]["reason"] == "unknown_question"
    assert result.get("record_context", {}).get("BUSINESS_NAME") == "Cafe Example"
    context = flagger.calls[0]["facts"].get("record_context")
    assert context and context.get("BUSINESS_NAME") == "Cafe Example"


def test_answer_question_flags_missing_value_when_column_empty() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "",
            "LOCATION_CITY": "",
        }
    ]
    agent, _, flagger = _make_agent(rows)

    result = agent.answer_question(
        ticket_id="T-4", question="What is the business name?", record_id="abc"
    )

    assert result["status"] == "missing_values"
    assert flagger.calls and "BUSINESS_NAME" in flagger.calls[0]["facts"]["missing_columns"]


def test_answer_question_emits_observability_events() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "Cafe Example",
            "LOCATION_CITY": "Florence",
        }
    ]
    logger = _LoggerStub()
    agent, _, _ = _make_agent(rows, logger=logger)

    result = agent.answer_question(
        ticket_id="T-obs", question="What is the business name?", record_id="abc"
    )

    assert result["status"] == "answered"
    events = [event for _, event, _ in logger.events]
    assert events[0] == "question_received"
    assert "sql_executed" in events
    assert "record_fetch_result" in events
    assert "columns_inferred" in events
    assert "facts_ready" in events
    assert events.count("question_resolved") == 1


class _LLMResponse:
    def __init__(self, text: str) -> None:
        self.output = [
            type(
                "Block",
                (),
                {
                    "content": [type("Text", (), {"text": text})()],
                },
            )()
        ]


class _LLMClientStub:
    def __init__(
        self,
        *,
        responses: list[str] | None = None,
        response_text: str | None = None,
    ) -> None:
        default_text = (
            response_text
            if response_text is not None
            else '{"status": "answered", "facts": [{"concept": "business_name", "value": "Example LLC"}]}'
        )
        self.calls: list[dict[str, Any]] = []
        self._responses: list[str] = list(responses) if responses is not None else []
        self._default_response: str = default_text

    def generate(
        self,
        *,
        messages: list[dict[str, str]],
        max_output_tokens: int | None = None,
        tools=None,
        response_format: dict[str, Any] | None = None,
    ) -> Any:  # type: ignore[override]
        self.calls.append({"messages": messages})
        if self._responses:
            text = self._responses.pop(0)
        else:
            text = self._default_response
        return _LLMResponse(text)


def test_query_agent_uses_llm_when_columns_missing() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "",
        }
    ]
    llm = _LLMClientStub(
        responses=[
            '{"columns": ["BUSINESS_NAME"]}',
            '{"status": "answered", "facts": [{"concept": "business_name", "value": "Example LLC"}]}',
        ]
    )
    agent, _, flagger = _make_agent(rows, llm_client=llm)

    result = agent.answer_question(
        ticket_id="T-llm", question="What is the business name?", record_id="abc"
    )

    assert result["status"] == "answered"
    facts = result.get("facts")
    assert isinstance(facts, list) and facts[0]["value"] == "Example LLC"
    assert not flagger.calls
    assert len(llm.calls) == 2
    selection_call = llm.calls[0]
    user_message = selection_call["messages"][1]["content"]
    assert "Available columns" in user_message
    assert "BUSINESS_NAME" in user_message
    assert result["answer_origin"] == "llm"


def test_query_agent_feeds_dataset_column_catalog_to_llm() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "Cafe Example",
        }
    ]
    dataset_columns = ["BRIZO_ID", "BUSINESS_NAME", "EMPLOYEE_COUNT"]
    llm = _LLMClientStub(responses=['{"columns": ["BUSINESS_NAME"]}'])
    agent, _, flagger = _make_agent(
        rows,
        llm_client=llm,
        dataset_columns=dataset_columns,
    )

    result = agent.answer_question(
        ticket_id="T-columns",
        question="What is the business name?",
        record_id="abc",
    )

    assert result["status"] == "answered"
    assert not flagger.calls
    assert len(llm.calls) == 1
    selection_user = llm.calls[0]["messages"][1]["content"]
    assert "EMPLOYEE_COUNT" in selection_user
    assert "BUSINESS_NAME" in selection_user


def test_query_agent_incorporates_scraper_findings_with_llm() -> None:
    rows = [
        {
            "BRIZO_ID": "abc",
            "BUSINESS_NAME": "Cafe Example",
        }
    ]
    follow_up_payload = json.dumps(
        {
            "status": "answered",
            "facts": [
                {
                    "concept": "employee_count",
                    "value": 1200,
                    "sources": ["https://example.com/report"],
                    "notes": "Derived from the annual report.",
                }
            ],
        }
    )
    llm = _LLMClientStub(response_text=follow_up_payload)
    agent, _, _ = _make_agent(rows, llm_client=llm)

    initial = agent.answer_question(
        ticket_id="T-follow",
        question="How many employees?",
        record_id="abc",
    )

    assert initial["status"] == "unknown_question"

    findings = [
        {
            "topic": "general",
            "query": "Cafe Example employees",
            "result": {
                "url": "https://example.com/report",
                "snippet": "Cafe Example employs 1,200 team members worldwide.",
            },
        }
    ]

    follow_up = agent.incorporate_scraper_findings(
        ticket_id="T-follow",
        question="How many employees?",
        record_id="abc",
        findings=findings,
        record_context=initial.get("record_context"),
    )

    assert follow_up is not None
    assert follow_up["status"] == "answered"
    facts = follow_up["facts"]
    assert facts[0]["concept"] == "employee_count"
    assert facts[0]["value"] == 1200
    assert follow_up["answer_origin"] == "scraper"
    assert follow_up["fact_sources"] == {"employee_count": "https://example.com/report"}
    assert "Derived" in facts[0].get("notes", "")
