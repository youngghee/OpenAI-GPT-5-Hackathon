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


def test_schema_agent_generates_columns_and_migration() -> None:
    writer = _MigrationWriterStub()
    agent = SchemaAgent(migration_writer=writer, table_name="dataset")

    proposal = agent.propose_change(
        ticket_id="T-5",
        evidence_summary={"unknown_fields": {"new_metric": 12.5, "flag": True}},
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
