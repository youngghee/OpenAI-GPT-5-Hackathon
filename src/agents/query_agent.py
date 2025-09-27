"""Entry point for answering user questions against CRM records.

This module exposes the `QueryAgent`, responsible for:
- Collecting row-level context from the data warehouse.
- Deciding when to invoke the enrichment workflow via `flag_missing`.
- Returning structured answers and rationale to the caller.

The concrete data access and tool invocation strategies will be provided by
small adapter objects to keep the orchestration logic thin and testable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol

TOKEN_MIN_LENGTH = 3

DEFAULT_SYNONYMS: dict[str, set[str]] = {
    "BUSINESS_NAME": {"business name", "name"},
    "LOCATION_CITY": {"city"},
    "RECORD_STATUS": {"status"},
    "LOCATION_STATE_CODE": {"state", "state code"},
    "LOCATION_COUNTRY": {"country"},
}


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
    primary_key_column: str = "BRIZO_ID"
    table_name: str = "dataset"
    max_columns: int = 3

    def answer_question(self, *, ticket_id: str, question: str, record_id: str) -> dict[str, Any]:
        """Return a structured answer for the provided question."""

        row = self._fetch_record(record_id)
        if row is None:
            self._flag_missing(
                ticket_id, question, {"reason": "record_not_found", "record_id": record_id}
            )
            return {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "record_not_found",
            }

        columns = self._infer_columns(question, row)
        if not columns:
            self._flag_missing(ticket_id, question, {"reason": "unknown_question"})
            return {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "unknown_question",
            }

        answers = {column: row[column] for column in columns if self._has_value(row.get(column))}
        if not answers:
            self._flag_missing(
                ticket_id,
                question,
                {"reason": "missing_values", "missing_columns": columns},
            )
            return {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "missing_values",
                "missing_columns": columns,
            }

        return {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "question": question,
            "status": "answered",
            "answers": answers,
        }

    def _fetch_record(self, record_id: str) -> dict[str, Any] | None:
        statement = self._build_select_statement(record_id)
        rows = self.sql_executor.run(statement)
        return rows[0] if rows else None

    def _build_select_statement(self, record_id: str) -> str:
        safe_id = record_id.replace("'", "''")
        return (
            f"SELECT * FROM {self.table_name} WHERE {self.primary_key_column} = '{safe_id}' LIMIT 1"
        )

    def _infer_columns(self, question: str, row: dict[str, Any]) -> list[str]:
        normalized_question = self._normalize(question)
        if not normalized_question:
            return []

        candidates: list[str] = []
        for column in row:
            synonyms = self._column_synonyms(column)
            if any(phrase in normalized_question for phrase in synonyms):
                candidates.append(column)
                continue

            normalized_column = self._normalize(column)
            if self._column_tokens_in_question(normalized_column, normalized_question):
                candidates.append(column)

        unique_candidates: list[str] = []
        for column in candidates:
            if column not in unique_candidates:
                unique_candidates.append(column)
            if len(unique_candidates) >= self.max_columns:
                break
        return unique_candidates

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True

    def _column_synonyms(self, column: str) -> set[str]:
        base = DEFAULT_SYNONYMS.get(column.upper(), set())
        derived = {
            self._normalize(column),
            column.replace("_", " ").lower(),
        }
        return {phrase for phrase in base.union(derived) if phrase}

    @staticmethod
    def _column_tokens_in_question(column: str, question: str) -> bool:
        if not column:
            return False
        column_tokens = {token for token in column.split() if len(token) >= TOKEN_MIN_LENGTH}
        if not column_tokens:
            return False
        question_tokens = set(question.split())
        return column_tokens.issubset(question_tokens)

    @staticmethod
    def _normalize(text: str) -> str:
        lowered = text.lower()
        cleaned = re.sub(r"[^a-z0-9\s]", " ", lowered)
        return re.sub(r"\s+", " ", cleaned).strip()

    def _flag_missing(self, ticket_id: str, question: str, facts: dict[str, Any]) -> None:
        self.missing_data_flagger.flag_missing(ticket_id=ticket_id, question=question, facts=facts)
