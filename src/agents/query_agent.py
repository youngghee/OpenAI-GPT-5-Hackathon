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
    dataset_columns: list[str] | None = None

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

        available_columns = self._list_available_columns(row)
        columns = self._select_columns(
            ticket_id=ticket_id,
            question=question,
            row=row,
            available_columns=available_columns,
        )
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

        facts = self._resolve_facts(ticket_id, question, row, columns)
        if not facts:
            missing_details = {"reason": "missing_values", "missing_columns": columns}
            if candidate_urls:
                missing_details["candidate_urls"] = candidate_urls
            if record_context:
                missing_details["record_context"] = record_context
            self._flag_missing(ticket_id, question, missing_details)
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
            "facts_ready",
            {
                "record_id": record_id,
                "concepts": [fact.get("concept") for fact in facts],
            },
        )
        result = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "question": question,
            "status": "answered",
            "facts": facts,
            "candidate_urls": candidate_urls,
        }
        answer_origin = self._determine_answer_origin(facts)
        if answer_origin:
            result["answer_origin"] = answer_origin
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
        facts = follow_up.get("facts") if follow_up else None

        if not facts:
            self._log_event(
                ticket_id,
                "scraper_follow_up_insufficient",
                {"reason": follow_up.get("status") if follow_up else "no_payload"},
            )
            return None

        self._log_event(
            ticket_id,
            "scraper_follow_up_answered",
            {
                "concepts": [fact.get("concept") for fact in facts if isinstance(fact, dict)],
            },
        )

        result: dict[str, Any] = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "question": question,
            "status": follow_up.get("status", "answered"),
            "facts": facts,
        }
        if candidate_urls:
            result.setdefault("candidate_urls", candidate_urls)
        if context:
            result.setdefault("record_context", context)

        if follow_up.get("sources"):
            result["aggregate_sources"] = follow_up["sources"]

        if follow_up.get("notes"):
            result["answer_notes"] = follow_up["notes"]

        fact_source_map = self._collect_fact_sources(facts)
        if fact_source_map:
            result["fact_sources"] = fact_source_map

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

    def _list_available_columns(self, row: dict[str, Any]) -> list[str]:
        if self.dataset_columns:
            # Preserve declared order while removing duplicates.
            ordered: list[str] = []
            seen: set[str] = set()
            for column in self.dataset_columns:
                key = column.upper()
                if key in seen:
                    continue
                seen.add(key)
                ordered.append(column)
            return ordered
        return list(row.keys())

    def _select_columns(
        self,
        *,
        ticket_id: str,
        question: str,
        row: dict[str, Any],
        available_columns: list[str],
    ) -> list[str]:
        lookup = self._build_column_lookup(set(available_columns))
        chosen: list[str] = []

        if self.llm_client is not None and available_columns:
            chosen = self._select_columns_with_llm(
                ticket_id=ticket_id,
                question=question,
                available_columns=available_columns,
                lookup=lookup,
            )

        if not chosen:
            chosen = self._infer_columns_from_question(question, row)

        normalized = self._normalize_column_candidates(chosen, lookup)
        if self.max_columns and len(normalized) > self.max_columns:
            return normalized[: self.max_columns]
        return normalized

    def _infer_columns_from_question(self, question: str, row: dict[str, Any]) -> list[str]:
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

    def _select_columns_with_llm(
        self,
        *,
        ticket_id: str,
        question: str,
        available_columns: list[str],
        lookup: dict[str, str],
    ) -> list[str]:
        prompt = self._build_column_selection_prompt(question, available_columns)
        try:
            response = self.llm_client.generate(messages=prompt)
        except Exception as exc:  # pragma: no cover - defensive fallback
            self._log_event(
                ticket_id,
                "llm_error",
                {"stage": "column_selection", "error": str(exc)},
            )
            return []

        selected = self._extract_column_selection(response, lookup)
        if selected:
            self._log_event(
                ticket_id,
                "columns_selected_by_llm",
                {"question": question, "columns": selected},
            )
        return selected

    def _resolve_facts(
        self,
        ticket_id: str,
        question: str,
        row: dict[str, Any],
        columns: list[str],
    ) -> list[dict[str, Any]]:
        direct_facts = self._collect_direct_facts(row, columns)
        if direct_facts:
            return direct_facts

        if self.llm_client is None:
            return []

        llm_facts = self._resolve_facts_with_llm(ticket_id, question, row)
        if llm_facts:
            return llm_facts
        return []

    def _collect_direct_facts(
        self, row: dict[str, Any], columns: list[str]
    ) -> list[dict[str, Any]]:
        facts: list[dict[str, Any]] = []
        for column in columns:
            value = row.get(column)
            if not self._has_value(value):
                continue
            facts.append(
                self._fact_from_column(
                    column=column,
                    value=value,
                    origin="dataset",
                    confidence=1.0,
                )
            )
        return facts

    def _fact_from_column(
        self,
        *,
        column: str,
        value: Any,
        origin: str,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        concept = self._column_to_concept(column)
        fact: dict[str, Any] = {
            "concept": concept,
            "value": value,
            "origin": origin,
        }
        if confidence is not None:
            fact["confidence"] = confidence
        fact["candidate_columns"] = [column]
        return fact

    def _resolve_facts_with_llm(
        self, ticket_id: str, question: str, row: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if self.llm_client is None:
            return []

        prompt = self._build_prompt(question, row)
        try:
            response = self.llm_client.generate(messages=prompt)
        except Exception as exc:  # pragma: no cover - defensive fallback
            self._log_event(
                ticket_id,
                "llm_error",
                {"stage": "direct_answer", "error": str(exc)},
            )
            return []

        facts = self._extract_facts_from_response(response)
        if facts:
            self._log_event(
                ticket_id,
                "llm_facts",
                {"concepts": [fact.get("concept") for fact in facts]},
            )
        return facts

    def _build_column_selection_prompt(
        self, question: str, columns: Sequence[str]
    ) -> list[dict[str, str]]:
        limit_clause = (
            f"Select up to {self.max_columns} column names that you need to inspect"
            if self.max_columns
            else "Select the column names that you need to inspect"
        )
        instructions = (
            "You are a CRM analyst preparing to answer a stakeholder's question."
            f" {limit_clause}. Choose only from the provided list of columns."
            " Respond with JSON containing a key 'columns' whose value is an array of"
            " column names (exact spellings from the list). You may optionally include a"
            " 'notes' field with short rationale."
        )
        column_lines = "\n".join(f"- {name}" for name in columns)
        user_content = (
            f"Question: {question}\nAvailable columns:\n{column_lines}\n"
            "Return the JSON object as described."
        )
        return [
            {"role": "system", "content": instructions},
            {"role": "user", "content": user_content},
        ]

    def _extract_column_selection(
        self, response: Any, lookup: dict[str, str]
    ) -> list[str]:
        for payload in self._iter_json_payloads(response):
            if not isinstance(payload, dict):
                continue

            raw_columns = payload.get("columns")
            if isinstance(raw_columns, list):
                candidates = [str(item) for item in raw_columns if item is not None]
                normalized = self._normalize_column_candidates(candidates, lookup)
                if normalized:
                    return normalized

            # Allow alternate key 'column_names' for robustness.
            fallback = payload.get("column_names")
            if isinstance(fallback, list):
                candidates = [str(item) for item in fallback if item is not None]
                normalized = self._normalize_column_candidates(candidates, lookup)
                if normalized:
                    return normalized

        return []

    def _build_prompt(self, question: str, row: dict[str, Any]) -> list[dict[str, str]]:
        context = json.dumps(row, ensure_ascii=False)
        return [
            {
                "role": "system",
                "content": (
                    "You are a CRM analyst. Use the record JSON to answer the"
                    " stakeholder's question. Respond with JSON containing 'status' and"
                    " 'facts'. 'status' must be 'answered' or 'insufficient'. 'facts'"
                    " must be an array of objects with the keys 'concept' (lower_snake_case"
                    " semantic label), 'value', optional 'confidence' (0-1), optional"
                    " 'notes', optional 'sources' (array of URLs or citations), and"
                    " optional 'column_hint' if an existing column clearly matches. If"
                    " the record does not contain the information, return 'status':"
                    " 'insufficient' with an empty facts array. Do not invent values."
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
            " the stakeholder's question. Respond with JSON containing 'status' and"
            " 'facts'. 'status' must be 'answered' or 'insufficient'. 'facts' must be"
            " an array where each item includes: 'concept' (lower_snake_case semantic"
            " label), 'value', optional 'confidence' (0-1), optional 'sources' (array"
            " of supporting URLs drawn from the evidence), optional 'notes', and"
            " optional 'column_hint' if an existing column clearly matches. Only"
            " include facts that the evidence supports; otherwise return an empty"
            " array with status 'insufficient'."
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
        column_lookup = self._build_column_lookup(row_keys)

        for payload in self._iter_json_payloads(response):
            if not isinstance(payload, dict):
                continue

            facts = self._parse_fact_payload(
                payload,
                origin="scraper",
                valid_columns=column_lookup,
            )

            status_value = payload.get("status")
            raw_status = (
                status_value.strip().lower()
                if isinstance(status_value, str) and status_value.strip()
                else None
            )
            if raw_status not in {"answered", "insufficient"}:
                raw_status = "answered" if facts else "insufficient"

            notes_value = payload.get("notes")
            notes = notes_value.strip() if isinstance(notes_value, str) and notes_value.strip() else None

            aggregate_sources = self._normalize_sources(payload.get("sources"))

            if facts or notes or aggregate_sources or raw_status:
                result: dict[str, Any] = {
                    "facts": facts,
                    "status": raw_status or "insufficient",
                }
                if notes:
                    result["notes"] = notes
                if aggregate_sources:
                    result["sources"] = aggregate_sources
                return result

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

    def _extract_facts_from_response(self, response: Any, *, origin: str = "llm") -> list[dict[str, Any]]:
        for payload in QueryAgent._iter_json_payloads(response):
            if not isinstance(payload, dict):
                continue
            facts = self._parse_fact_payload(payload, origin=origin, valid_columns=None)
            if facts:
                return facts
        return []

    def _parse_fact_payload(
        self,
        payload: dict[str, Any],
        *,
        origin: str,
        valid_columns: dict[str, str] | None,
    ) -> list[dict[str, Any]]:
        facts_section = payload.get("facts")
        if not isinstance(facts_section, list):
            return []

        resolved: list[dict[str, Any]] = []
        for entry in facts_section:
            if not isinstance(entry, dict):
                continue
            concept_raw = entry.get("concept")
            value = entry.get("value")
            if not isinstance(concept_raw, str) or not self._has_value(value):
                continue
            concept = self._canonicalise_concept(concept_raw)
            if not concept:
                continue

            fact: dict[str, Any] = {
                "concept": concept,
                "value": value,
                "origin": origin,
            }

            confidence = entry.get("confidence")
            if isinstance(confidence, (int, float)):
                fact["confidence"] = max(0.0, min(1.0, float(confidence)))
            elif origin != "dataset":
                fact["confidence"] = 0.6

            notes = entry.get("notes")
            if isinstance(notes, str) and notes.strip():
                fact["notes"] = notes.strip()

            sources = entry.get("sources")
            formatted_sources = self._normalize_sources(sources)
            if formatted_sources:
                fact["sources"] = formatted_sources

            candidate_columns: list[str] = []
            raw_candidates = entry.get("candidate_columns")
            if isinstance(raw_candidates, (list, tuple)):
                candidate_columns.extend(
                    str(item).strip()
                    for item in raw_candidates
                    if isinstance(item, (str, int, float)) and str(item).strip()
                )
            else:
                column_hint = (
                    entry.get("column_hint")
                    or entry.get("field")
                    or entry.get("column")
                    or entry.get("target")
                )
                if isinstance(column_hint, str) and column_hint.strip():
                    candidate_columns.append(column_hint.strip())

            if candidate_columns:
                normalized = self._normalize_column_candidates(candidate_columns, valid_columns)
                if normalized:
                    fact["candidate_columns"] = normalized

            resolved.append(fact)

        return resolved

    @staticmethod
    def _normalize_sources(sources: Any) -> list[str]:
        normalized: list[str] = []
        if isinstance(sources, (list, tuple)):
            iterable = sources
        else:
            iterable = [sources]

        for item in iterable:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            normalized.append(text)

        return normalized

    def _normalize_column_candidates(
        self, candidates: list[str], valid_columns: dict[str, str] | None
    ) -> list[str]:
        unique: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            resolved = self._resolve_column_hint(candidate, valid_columns)
            if not resolved:
                continue
            if resolved not in seen:
                seen.add(resolved)
                unique.append(resolved)
        return unique

    @staticmethod
    def _resolve_column_hint(candidate: str, valid_columns: dict[str, str] | None) -> str | None:
        cleaned = candidate.strip()
        if not cleaned:
            return None
        if not valid_columns:
            return cleaned

        upper = cleaned.upper()
        if upper in valid_columns:
            return valid_columns[upper]

        sanitized = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
        if sanitized in valid_columns:
            return valid_columns[sanitized]

        return cleaned

    @staticmethod
    def _canonicalise_concept(concept: str) -> str:
        lowered = concept.strip().lower()
        if not lowered:
            return ""
        return re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")

    @staticmethod
    def _build_column_lookup(columns: set[str]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for column in columns:
            label = column.strip()
            if not label:
                continue
            upper = label.upper()
            lookup[upper] = column
            sanitized = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
            if sanitized:
                lookup[sanitized] = column
        return lookup

    @staticmethod
    def _collect_fact_sources(facts: Sequence[dict[str, Any]]) -> dict[str, str]:
        sources_map: dict[str, str] = {}
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            concept = fact.get("concept")
            sources = fact.get("sources")
            if not isinstance(concept, str) or not concept:
                continue
            if isinstance(sources, (list, tuple)):
                for candidate in sources:
                    text = str(candidate).strip() if candidate is not None else ""
                    if text:
                        sources_map.setdefault(concept, text)
                        break
            elif isinstance(sources, str) and sources.strip():
                sources_map.setdefault(concept, sources.strip())
        return sources_map

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True

    @staticmethod
    def _determine_answer_origin(facts: Sequence[dict[str, Any]]) -> str | None:
        origins = {
            fact.get("origin")
            for fact in facts
            if isinstance(fact, dict) and fact.get("origin")
        }
        if not origins:
            return None
        if origins == {"dataset"}:
            return "dataset"
        if origins == {"scraper"}:
            return "scraper"
        if origins == {"llm"}:
            return "llm"
        return "mixed"

    def _column_synonyms(self, column: str) -> set[str]:
        base = DEFAULT_SYNONYMS.get(column.upper(), set())
        derived = {
            self._normalize(column),
            column.replace("_", " ").lower(),
        }
        return {phrase for phrase in base.union(derived) if phrase}

    @staticmethod
    def _column_to_concept(column: str) -> str:
        normalized = QueryAgent._normalize(column)
        if not normalized:
            return column.strip().lower()
        return normalized.replace(" ", "_")

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
