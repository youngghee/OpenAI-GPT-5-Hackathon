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
from typing import Any, Protocol, Sequence

from src.core.observability import QueryObservationSink
from src.core.record_utils import (
    build_record_context,
    extract_candidate_urls,
)
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
    candidate_url_fields: list[str] | None = None
    context_columns: list[str] | None = None

    def __post_init__(self) -> None:
        # Cache the last fetched row so follow-up stages can reuse it without
        # issuing duplicate SQL queries.
        self._last_row: dict[str, Any] | None = None
        self._last_record_id: str | None = None

    def answer_question(self, *, ticket_id: str, question: str, record_id: str) -> dict[str, Any]:
        """Return a structured answer for the provided question."""

        self._log_event(
            ticket_id,
            "question_received",
            {"record_id": record_id, "question": question},
        )

        row = self._fetch_record(ticket_id, record_id)
        self._last_row = row
        self._last_record_id = record_id
        candidate_urls = extract_candidate_urls(row, self.candidate_url_fields)
        record_context = build_record_context(row, self.context_columns)
        if row is None:
            self._flag_missing(
                ticket_id, question, {"reason": "record_not_found", "record_id": record_id}
            )
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "record_not_found",
                "candidate_urls": candidate_urls,
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
            facts = {"reason": "unknown_question"}
            if candidate_urls:
                facts["candidate_urls"] = candidate_urls
            if record_context:
                facts["record_context"] = record_context
            self._flag_missing(ticket_id, question, facts)
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "unknown_question",
                "candidate_urls": candidate_urls,
                "record_context": record_context,
            }
            self._log_event(
                ticket_id,
                "question_resolved",
                {"record_id": record_id, "status": result["status"]},
            )
            return result

        answers = self._resolve_answers(ticket_id, question, row, columns)
        if not answers:
            facts = {"reason": "missing_values", "missing_columns": columns}
            if candidate_urls:
                facts["candidate_urls"] = candidate_urls
            if record_context:
                facts["record_context"] = record_context
            self._flag_missing(ticket_id, question, facts)
            result = {
                "ticket_id": ticket_id,
                "record_id": record_id,
                "question": question,
                "status": "missing_values",
                "missing_columns": columns,
                "candidate_urls": candidate_urls,
                "record_context": record_context,
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
            "candidate_urls": candidate_urls,
        }
        if record_context:
            result["record_context"] = record_context
        self._log_event(
            ticket_id,
            "question_resolved",
            {"record_id": record_id, "status": result["status"]},
        )
        return result

    def incorporate_scraper_findings(
        self,
        *,
        ticket_id: str,
        question: str,
        record_id: str,
        findings: Sequence[dict[str, Any]],
        record_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Use external evidence to attempt a follow-up answer."""

        if not findings:
            return None

        self._log_event(
            ticket_id,
            "scraper_findings_received",
            {"record_id": record_id, "finding_count": len(findings)},
        )

        if self.llm_client is None:
            self._log_event(
                ticket_id,
                "scraper_follow_up_skipped",
                {"reason": "llm_unavailable"},
            )
            return None

        row = self._get_cached_row(record_id)
        if row is None:
            row = self._fetch_record(ticket_id, record_id)
            self._last_row = row
            self._last_record_id = record_id

        candidate_urls = extract_candidate_urls(row, self.candidate_url_fields)
        context = record_context or build_record_context(row, self.context_columns)

        prompt = self._build_follow_up_prompt(
            question=question,
            row=row or {},
            record_context=context,
            findings=findings,
        )

        self._log_event(
            ticket_id,
            "scraper_follow_up_attempted",
            {"question": question, "finding_count": len(findings)},
        )

        try:
            response = self.llm_client.generate(messages=prompt)
        except Exception as exc:  # pragma: no cover - defensive fallback
            self._log_event(
                ticket_id,
                "llm_error",
                {"stage": "scraper_follow_up", "error": str(exc)},
            )
            return None

        row_keys = set(row.keys()) if isinstance(row, dict) else set()
        follow_up = self._extract_follow_up_response(response, row_keys)
        answers = follow_up.get("answers") if follow_up else None

        if not answers:
            self._log_event(
                ticket_id,
                "scraper_follow_up_insufficient",
                {"reason": follow_up.get("status") if follow_up else "no_payload"},
            )
            return None

        self._log_event(
            ticket_id,
            "scraper_follow_up_answered",
            {"fields": list(answers.keys())},
        )

        result: dict[str, Any] = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "question": question,
            "status": follow_up.get("status", "answered"),
            "answers": answers,
        }
        if candidate_urls:
            result.setdefault("candidate_urls", candidate_urls)
        if context:
            result.setdefault("record_context", context)

        sources = follow_up.get("sources")
        if sources:
            result["answer_sources"] = sources

        notes = follow_up.get("notes")
        if notes:
            result["answer_notes"] = notes

        result["answer_origin"] = "scraper"
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

    def _get_cached_row(self, record_id: str) -> dict[str, Any] | None:
        if getattr(self, "_last_record_id", None) == record_id:
            return getattr(self, "_last_row", None)
        return None

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

        allowed_fields = set(row.keys())
        answers = self._extract_answers_from_response(response, allowed_fields)
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

    def _build_follow_up_prompt(
        self,
        *,
        question: str,
        row: dict[str, Any],
        record_context: dict[str, Any] | None,
        findings: Sequence[dict[str, Any]],
    ) -> list[dict[str, str]]:
        columns = sorted(row.keys())
        row_snapshot = json.dumps(row, ensure_ascii=False) if row else "{}"
        context_snapshot = json.dumps(record_context or {}, ensure_ascii=False)
        evidence_text = self._format_findings(findings)
        columns_text = ", ".join(columns) if columns else "none"
        instructions = (
            "You are a CRM analyst. Use the record context and external evidence to answer"
            " the stakeholder's question. Respond with JSON containing the keys"
            " 'status', 'answers', 'sources', and 'notes'. 'status' must be either"
            " 'answered' or 'insufficient'. 'answers' maps FIELD_NAME to value, using"
            " UPPER_SNAKE_CASE and reusing existing column names when applicable."
            " 'sources' maps the same field names to the best supporting URL."
            " 'notes' provides a short rationale and may be omitted if unnecessary."
            " Only include answers that the evidence supports."
        )
        user_content = (
            f"Question: {question}\n"
            f"Known record columns: {columns_text}\n"
            f"Record context: {context_snapshot}\n"
            f"Record snapshot: {row_snapshot}\n"
            "Evidence:\n"
            f"{evidence_text}\n"
            "Return the JSON object as described."
        )
        return [
            {"role": "system", "content": instructions},
            {"role": "user", "content": user_content},
        ]

    def _extract_follow_up_response(
        self, response: Any, row_keys: set[str]
    ) -> dict[str, Any]:  # pragma: no cover - thin wrapper around parsing helper
        lookup: dict[str, str] = {}
        for key in row_keys:
            upper = key.upper()
            lookup[upper] = key
            sanitized = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
            if sanitized:
                lookup[sanitized] = key

        for payload in self._iter_json_payloads(response):
            answers_section = payload.get("answers") if isinstance(payload, dict) else None
            if isinstance(answers_section, dict):
                answer_candidates = answers_section
            else:
                answer_candidates = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"status", "sources", "notes"}
                }

            answers: dict[str, Any] = {}
            for key, value in answer_candidates.items():
                if not self._has_value(value):
                    continue
                canonical = self._canonicalise_field_name(str(key), lookup)
                answers[canonical] = value

            raw_sources = payload.get("sources") if isinstance(payload, dict) else None
            sources: dict[str, str] = {}
            if isinstance(raw_sources, dict):
                for key, value in raw_sources.items():
                    if not isinstance(value, str) or not value.strip():
                        continue
                    canonical = self._canonicalise_field_name(str(key), lookup)
                    sources[canonical] = value.strip()

            notes_value = payload.get("notes") if isinstance(payload, dict) else None
            notes = notes_value.strip() if isinstance(notes_value, str) and notes_value.strip() else None

            status_value = payload.get("status") if isinstance(payload, dict) else None
            raw_status = status_value.strip().lower() if isinstance(status_value, str) and status_value.strip() else None
            if raw_status not in {"answered", "insufficient"}:
                raw_status = "answered" if answers else "insufficient"

            if answers or notes or sources or raw_status:
                return {
                    "answers": answers,
                    "sources": sources,
                    "notes": notes,
                    "status": raw_status or "insufficient",
                }
        return {}

    @staticmethod
    def _format_findings(findings: Sequence[dict[str, Any]]) -> str:
        lines: list[str] = []
        for index, entry in enumerate(findings, start=1):
            parts: list[str] = []
            topic = entry.get("topic") if isinstance(entry, dict) else None
            if isinstance(topic, str) and topic.strip():
                parts.append(f"Topic: {topic.strip()}")
            query = entry.get("query") if isinstance(entry, dict) else None
            if isinstance(query, str) and query.strip():
                parts.append(f"Query: {query.strip()}")
            result = entry.get("result") if isinstance(entry, dict) else None
            if isinstance(result, dict):
                url = result.get("url") or result.get("link")
                if isinstance(url, str) and url.strip():
                    parts.append(f"URL: {url.strip()}")
                title = result.get("title") or result.get("name")
                if isinstance(title, str) and title.strip():
                    parts.append(f"Title: {QueryAgent._truncate_text(title.strip())}")
                snippet = (
                    result.get("snippet")
                    or result.get("text")
                    or result.get("description")
                )
                if isinstance(snippet, str) and snippet.strip():
                    parts.append(f"Snippet: {QueryAgent._truncate_text(snippet.strip())}")
            elif result is not None:
                parts.append(f"Result: {QueryAgent._truncate_text(str(result))}")

            if not parts:
                parts.append(f"Raw entry: {QueryAgent._truncate_text(str(entry))}")

            lines.append(f"{index}. {'; '.join(parts)}")
        return "\n".join(lines)

    @staticmethod
    def _truncate_text(value: str, limit: int = 320) -> str:
        trimmed = value.strip()
        if len(trimmed) <= limit:
            return trimmed
        return trimmed[: limit - 3] + "..."

    @staticmethod
    def _iter_json_payloads(response: Any) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for text in QueryAgent._extract_response_text_blocks(response):
            for candidate in QueryAgent._decode_json_strings(text):
                payloads.append(candidate)
        return payloads

    @staticmethod
    def _decode_json_strings(text: str) -> list[dict[str, Any]]:
        cleaned = text.strip()
        if not cleaned:
            return []

        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\n", "", cleaned)
            cleaned = re.sub(r"```\s*$", "", cleaned)

        decoder = json.JSONDecoder()
        index = 0
        results: list[dict[str, Any]] = []
        length = len(cleaned)

        while index < length:
            char = cleaned[index]
            if char not in "[{":
                index += 1
                continue
            try:
                value, offset = decoder.raw_decode(cleaned, index)
            except json.JSONDecodeError:
                index += 1
                continue
            index = offset
            if isinstance(value, dict):
                results.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        results.append(item)

        return results

    @staticmethod
    def _extract_response_text_blocks(response: Any) -> list[str]:
        seen: set[str] = set()
        texts: list[str] = []
        output = getattr(response, "output", [])
        for item in output:
            content = getattr(item, "content", None)
            if isinstance(content, list):
                for block in content:
                    text = getattr(block, "text", "")
                    if isinstance(text, str) and QueryAgent._remember_text(text, seen):
                        texts.append(text.strip())
            elif isinstance(content, dict):
                text = content.get("text") or content.get("output_text")
                if isinstance(text, str) and QueryAgent._remember_text(text, seen):
                    texts.append(text.strip())
            elif content is not None:
                text = getattr(content, "text", "")
                if isinstance(text, str) and QueryAgent._remember_text(text, seen):
                    texts.append(text.strip())
        for attr in ("output_text", "text"):
            raw = getattr(response, attr, None)
            if isinstance(raw, str):
                if QueryAgent._remember_text(raw, seen):
                    texts.append(raw.strip())
            elif isinstance(raw, (list, tuple)):
                for value in raw:
                    if isinstance(value, str) and QueryAgent._remember_text(value, seen):
                        texts.append(value.strip())
        return texts

    @staticmethod
    def _remember_text(text: str, seen: set[str]) -> bool:
        normalized = text.strip()
        if not normalized or normalized in seen:
            return False
        seen.add(normalized)
        return True

    @staticmethod
    def _canonicalise_field_name(field: str, lookup: dict[str, str]) -> str:
        candidate = field.strip()
        if not candidate:
            return candidate
        upper = candidate.upper()
        if upper in lookup:
            return lookup[upper]
        sanitized = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
        if sanitized in lookup:
            return lookup[sanitized]
        return sanitized or upper

    @staticmethod
    def _extract_answers_from_response(
        response: Any, allowed_fields: set[str] | None
    ) -> dict[str, Any]:  # pragma: no cover - thin wrapper
        for payload in QueryAgent._iter_json_payloads(response):
            candidate = payload.get("answers") if isinstance(payload, dict) else None
            answers_dict = candidate if isinstance(candidate, dict) else payload
            filtered: dict[str, Any] = {}
            for key, value in answers_dict.items():
                if not QueryAgent._has_value(value):
                    continue
                if allowed_fields is not None and key not in allowed_fields:
                    continue
                filtered[key] = value
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
