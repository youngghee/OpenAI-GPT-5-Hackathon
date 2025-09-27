"""Entry point for answering user questions against CRM records.

This module exposes the `QueryAgent`, responsible for:
- Collecting row-level context from the data warehouse.
- Deciding when to invoke the enrichment workflow via `flag_missing`.
- Returning structured answers and rationale to the caller.

The concrete data access and tool invocation strategies will be provided by
small adapter objects to keep the orchestration logic thin and testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SQLExecutor(Protocol):
    """Abstracts a SQL execution engine (e.g., Codex interpreter)."""

    def run(self, statement: str) -> list[dict[str, Any]]:  # pragma: no cover - interface
        """Execute a SQL statement and return row dictionaries."""


class MissingDataFlagger(Protocol):
    """Dispatch hook used when the agent identifies a data gap."""

    def flag_missing(
        self, ticket_id: str, question: str, facts: dict[str, Any]
    ) -> None:  # pragma: no cover - interface
        """Emit a signal for the scraper workflow with helpful context."""


@dataclass
class QueryAgent:
    """Coordinates data retrieval and enrichment for a single user question."""

    sql_executor: SQLExecutor
    missing_data_flagger: MissingDataFlagger

    def answer_question(self, *, ticket_id: str, question: str, record_id: str) -> dict[str, Any]:
        """Return an answer payload for the provided question.

        Parameters
        ----------
        ticket_id:
            Stable identifier that traces the question through downstream agents.
        question:
            The natural-language prompt supplied by the stakeholder.
        record_id:
            Primary key pointing at the CRM row under investigation.
        """

        # TODO: implement query evaluation, enrichment loop, and reasoning trace capture.
        raise NotImplementedError("QueryAgent.answer_question is pending implementation")
