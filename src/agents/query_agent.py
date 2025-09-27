"""Entry point for answering user questions against CRM records.

This module exposes the `QueryAgent`, responsible for:
- Collecting row-level context from the data warehouse.
- Deciding when to invoke the enrichment workflow via `flag_missing`.
- Returning structured answers and rationale to the caller.

The concrete data access and tool invocation strategies will be provided by
small adapter objects to keep the orchestration logic thin and testable.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

from src.core.observability import QueryObservationSink
from src.integrations.openai_agent_sdk import OpenAIAgentAdapter

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
    llm_client: OpenAIAgentAdapter | None = None
    primary_key_column: str = "BRIZO_ID"
    table_name: str = "dataset"
    max_columns: int = 3
    logger: QueryObservationSink | None = None

    def answer_question(self, *, ticket_id: str, question: str, record_id: str) -> dict[str, Any]:
        """Return a structured answer for the provided question."""

        self._log_event(
            ticket_id,
            "question_received",
            {"record_id": record_id, "question": question},
        )

        row = self._fetch_record(ticket_id, record_id)
        if row is None:
            self._flag_missing(
                ticket_id, question, {"reason": "record_not_found", "record_id": record_id}
            )
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "record_not_found",
            }
            self._log_event(
                ticket_id,
                "question_resolved",
                {"record_id": record_id, "status": result["status"]},
            )
            return result

        columns = self._infer_columns(question, row)
        self._log_event(
            ticket_id,
            "columns_inferred",
            {"record_id": record_id, "columns": columns},
        )
        if not columns:
            self._flag_missing(ticket_id, question, {"reason": "unknown_question"})
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "unknown_question",
            }
            self._log_event(
                ticket_id,
                "question_resolved",
                {"record_id": record_id, "status": result["status"]},
            )
            return result

        answers = self._resolve_answers(ticket_id, question, row, columns)
        if not answers:
            self._flag_missing(
                ticket_id,
                question,
                {"reason": "missing_values", "missing_columns": columns},
            )
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "missing_values",
                "missing_columns": columns,
            }
            self._log_event(
                ticket_id,
                "question_resolved",
                {"record_id": record_id, "status": result["status"]},
            )
            return result

        self._log_event(
            ticket_id,
            "answer_ready",
            {"record_id": record_id, "columns": list(answers.keys())},
        )
        result = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "question": question,
            "status": "answered",
            "answers": answers,
        }
        self._log_event(
            ticket_id,
            "question_resolved",
            {"record_id": record_id, "status": result["status"]},
        )
        return result

    def _fetch_record(self, ticket_id: str, record_id: str) -> dict[str, Any] | None:
        statement = self._build_select_statement(record_id)
        self._log_event(
            ticket_id,
            "sql_executed",
            {"record_id": record_id, "statement": statement},
        )
        rows = self.sql_executor.run(statement)
        found = rows[0] if rows else None
        self._log_event(
            ticket_id,
            "record_fetch_result",
            {"record_id": record_id, "found": bool(found)},
        )
        return found

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

    def _resolve_answers(
        self,
        ticket_id: str,
        question: str,
        row: dict[str, Any],
        columns: list[str],
    ) -> dict[str, Any]:
        if self.llm_client:
            llm_answers = self._resolve_answers_with_llm(ticket_id, question, row)
            if llm_answers:
                return llm_answers

        direct_answers = {column: row[column] for column in columns if self._has_value(row.get(column))}
        return direct_answers

    def _resolve_answers_with_llm(
        self, ticket_id: str, question: str, row: dict[str, Any]
    ) -> dict[str, Any]:
        if self.llm_client is None:
            return {}

        prompt = self._build_prompt(question, row)
        try:
            response = self.llm_client.generate(messages=prompt)
        except Exception as exc:  # pragma: no cover - defensive fallback
            self._log_event(
                ticket_id,
                "llm_error",
                {"error": str(exc)},
            )
            return {}

        answers = self._extract_answers_from_response(response, row)
        if answers:
            self._log_event(
                ticket_id,
                "llm_answer",
                {"columns": list(answers.keys())},
            )
        return answers

    def _build_prompt(self, question: str, row: dict[str, Any]) -> list[dict[str, str]]:
        context = json.dumps(row, ensure_ascii=False)
        return [
            {
                "role": "system",
                "content": (
                    "You are a CRM analyst. Use the provided record JSON to answer the"
                    " stakeholder's question. Respond with a JSON object whose keys are"
                    " relevant column names from the record and whose values are the"
                    " corresponding answers. If information is missing, omit the key."
                ),
            },
            {
                "role": "user",
                "content": f"Record JSON: {context}\nQuestion: {question}",
            },
        ]

    @staticmethod
    def _extract_answers_from_response(response: Any, row: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover - thin wrapper
        output = getattr(response, "output", [])
        candidates: list[str] = []
        for item in output:
            content = getattr(item, "content", None)
            if not content:
                continue
            if isinstance(content, list):
                for block in content:
                    text = getattr(block, "text", "")
                    if text:
                        candidates.append(text)
            else:
                text = getattr(content, "text", "")
                if text:
                    candidates.append(text)

        for text in candidates:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                filtered = {
                    key: value
                    for key, value in payload.items()
                    if key in row and QueryAgent._has_value(value)
                }
                if filtered:
                    return filtered
        return {}

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
        self._log_event(
            ticket_id,
            "missing_data_flagged",
            {"question": question, "facts": facts},
        )
        self.missing_data_flagger.flag_missing(ticket_id=ticket_id, question=question, facts=facts)

    def _log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:
        if self.logger is None:
            return
        try:
            self.logger.log_event(ticket_id, event, payload)
        except Exception:
            # Observability failures must not impact question handling.
            pass
