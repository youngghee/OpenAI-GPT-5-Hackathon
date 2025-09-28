"""Tests for schema agent proposals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.agents.schema_agent import SchemaAgent


@dataclass
class _MigrationWriterStub:
    calls: list[dict[str, Any]] = field(default_factory=list)
    path: str = "schema/migrations/20240101000000_ticket.sql"

    def write_migration(self, *, name: str, statements: list[str]) -> str:  # type: ignore[override]
        self.calls.append({"name": name, "statements": statements})
        return self.path


class _LLMStub:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[dict[str, Any]] = []

    def generate(self, *, messages, max_output_tokens=None, tools=None):  # type: ignore[override]
        self.calls.append({"messages": messages})

        class _Response:
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

        return _Response(self.text)


def test_schema_agent_generates_columns_and_migration() -> None:
    writer = _MigrationWriterStub()
    agent = SchemaAgent(migration_writer=writer, table_name="dataset")

    proposal = agent.propose_change(
        ticket_id="T-5",
        evidence_summary={
            "unmatched_facts": [
                {"concept": "new_metric", "value": 12.5},
                {"concept": "flag", "value": True},
            ]
        },
    )

    assert proposal["migration_path"] == writer.path
    expected_columns = 2
    assert len(proposal["columns"]) == expected_columns
    assert proposal["columns"][0]["name"] == "NEW_METRIC"
    assert any("NUMERIC" in stmt for stmt in proposal["migration_statements"])
    assert writer.calls and writer.calls[0]["name"] == "ticket_t-5"


def test_schema_agent_handles_no_unknown_fields() -> None:
    writer = _MigrationWriterStub()
    agent = SchemaAgent(migration_writer=writer)

    proposal = agent.propose_change(ticket_id="T-6", evidence_summary={})

    assert proposal["columns"] == []
    assert proposal["migration_path"] is None
    assert writer.calls == []


def test_schema_agent_uses_llm_proposals() -> None:
    writer = _MigrationWriterStub()
    llm = _LLMStub('[{"name": "engagement_score", "data_type": "numeric", "nullable": false, "description": "Score from 0-1"}]')
    agent = SchemaAgent(migration_writer=writer, llm_client=llm)

    proposal = agent.propose_change(
        ticket_id="T-llm",
        evidence_summary={
            "unmatched_facts": [{"concept": "Engagement Score", "value": 0.42}]
        },
    )

    assert proposal["columns"][0]["name"] == "ENGAGEMENT_SCORE"
    assert llm.calls
