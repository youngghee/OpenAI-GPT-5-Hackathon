"""Behavioural tests for the query agent."""

from __future__ import annotations

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
    rows: list[dict[str, Any]], logger: QueryObservationSink | None = None
) -> tuple[QueryAgent, _SQLExecutorStub, _FlaggerStub]:
    executor = _SQLExecutorStub(rows=rows)
    flagger = _FlaggerStub()
    return (
        QueryAgent(sql_executor=executor, missing_data_flagger=flagger, logger=logger),
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
    assert result["answers"] == {"BUSINESS_NAME": "Cafe Example"}
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
    assert events.count("question_resolved") == 1
