"""Update agent that reconciles enriched facts back into the CRM."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence

from src.integrations.openai_agent_sdk import OpenAIAgentAdapter


class CRMClient(Protocol):
    """API surface for interacting with the source-of-truth system."""

    def update_record(
        self, record_id: str, payload: dict[str, str]
    ) -> None:  # pragma: no cover - interface
        """Persist deterministic updates for a CRM record."""


class SchemaEscalator(Protocol):
    """Raised when enriched data does not map onto existing columns."""

    def escalate(
        self, ticket_id: str, rationale: dict[str, Any]
    ) -> None:  # pragma: no cover - interface
        """Forward schema-change requests downstream."""


@dataclass(slots=True)
class UpdateAgent:
    """Performs reconciliation after the query agent produces a satisfactory answer."""

    crm_client: CRMClient
    schema_escalator: SchemaEscalator
    allowed_fields: set[str] | None = field(default=None)
    llm_client: OpenAIAgentAdapter | None = field(default=None)
    _column_lookup: dict[str, str] = field(init=False, default_factory=dict)
    _column_tokens: dict[str, set[str]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        normalized_fields: set[str] | None = None
        if self.allowed_fields is not None:
            normalized_fields = {
                field.strip().upper()
                for field in self.allowed_fields
                if isinstance(field, str) and field.strip()
            }
            object.__setattr__(self, "allowed_fields", normalized_fields)

        lookup = self._build_column_lookup(normalized_fields or set())
        tokens = self._build_column_tokens(normalized_fields or set())
        object.__setattr__(self, "_column_lookup", lookup)
        object.__setattr__(self, "_column_tokens", tokens)

    def apply_enrichment(
        self,
        *,
        ticket_id: str,
        record_id: str,
        facts: Sequence[dict[str, Any]] | dict[str, Any],
    ) -> dict[str, Any]:
        """Attempt to persist semantic facts and escalate when schema gaps occur."""

        fact_list = self._coerce_fact_sequence(facts)

        summary: dict[str, Any] = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "fact_count": len(fact_list),
        }

        if not fact_list:
            summary["status"] = "skipped"
            return summary

        applied_updates: dict[str, Any] = {}
        applied_details: list[dict[str, Any]] = []
        unmatched_facts: list[dict[str, Any]] = []
        empty_facts: list[dict[str, Any]] = []

        for fact in fact_list:
            concept = str(fact.get("concept") or "").strip()
            value = fact.get("value")
            candidates = self._coerce_candidate_columns(fact.get("candidate_columns"))

            if not self._has_value(value):
                empty_facts.append(
                    {
                        "concept": concept,
                        "candidate_columns": candidates,
                    }
                )
                continue

            column = self._match_fact_to_column(concept, candidates)

            if column is not None:
                applied_updates[column] = value
                applied_details.append(
                    {
                        "concept": concept or column,
                        "column": column,
                        "value": value,
                        "candidates": candidates,
                    }
                )
            else:
                unmatched_facts.append(
                    {
                        "concept": concept,
                        "value": value,
                        "candidate_columns": candidates,
                    }
                )

        if applied_updates:
            self.crm_client.update_record(record_id, applied_updates)
            summary["status"] = "updated"
            summary["applied_columns"] = sorted(applied_updates.keys())
            summary["applied_facts"] = applied_details
        else:
            summary["status"] = "skipped"

        escalation_payload: dict[str, Any] = {}
        if unmatched_facts:
            escalation_payload["unmatched_facts"] = unmatched_facts
        if empty_facts:
            escalation_payload["empty_facts"] = empty_facts

        if escalation_payload:
            self.schema_escalator.escalate(ticket_id, escalation_payload)
            summary["escalated"] = escalation_payload

        reasoning = self._generate_reasoning(
            ticket_id=ticket_id,
            record_id=record_id,
            applied=applied_details,
            unmatched=unmatched_facts,
            empty=empty_facts,
        )
        if reasoning:
            summary["reasoning"] = reasoning

        return summary

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, dict, set, tuple)):
            return len(value) > 0
        return True

    @staticmethod
    def _coerce_fact_sequence(
        facts: Sequence[dict[str, Any]] | dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        if facts is None:
            return []
        if isinstance(facts, dict):
            return [
                {"concept": key, "value": value}
                for key, value in facts.items()
            ]

        coerced: list[dict[str, Any]] = []
        for entry in facts:
            if isinstance(entry, dict):
                coerced.append(entry)
        return coerced

    @staticmethod
    def _coerce_candidate_columns(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, (list, tuple, set)):
            values = list(raw)
        else:
            values = [raw]

        candidates: list[str] = []
        for value in values:
            text = str(value).strip() if value is not None else ""
            if text:
                candidates.append(text)
        return candidates

    def _match_fact_to_column(
        self, concept: str, candidate_columns: Sequence[str]
    ) -> str | None:
        if self.allowed_fields is None:
            # Fallback: trust provided hints or derive from concept.
            for candidate in candidate_columns:
                normalized = self._normalize_label(candidate)
                if normalized:
                    return normalized
            normalized_concept = self._normalize_label(concept)
            return normalized_concept or None

        for candidate in candidate_columns:
            resolved = self._resolve_candidate_column(candidate)
            if resolved is not None:
                return resolved

        normalized_concept = self._normalize_label(concept)
        if normalized_concept and normalized_concept in self._column_lookup:
            return self._column_lookup[normalized_concept]

        concept_tokens = self._tokens_from_label(concept)
        if concept_tokens:
            matches = [
                column
                for column, tokens in self._column_tokens.items()
                if concept_tokens.issubset(tokens)
            ]
            if len(matches) == 1:
                return matches[0]

        return None

    def _resolve_candidate_column(self, candidate: str) -> str | None:
        normalized = self._normalize_label(candidate)
        if not normalized:
            return None
        return self._column_lookup.get(normalized)

    @staticmethod
    def _normalize_label(label: str | None) -> str:
        if label is None:
            return ""
        if not isinstance(label, str):
            label = str(label)
        return re.sub(r"[^A-Z0-9]+", "_", label.upper()).strip("_")

    @staticmethod
    def _tokens_from_label(label: str | None) -> set[str]:
        normalized = UpdateAgent._normalize_label(label)
        if not normalized:
            return set()
        return {token.lower() for token in normalized.split("_") if token}

    @staticmethod
    def _build_column_lookup(columns: set[str]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for column in columns:
            if not column:
                continue
            upper = column.upper()
            lookup[upper] = upper
            sanitized = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
            if sanitized:
                lookup[sanitized] = upper
        return lookup

    @staticmethod
    def _build_column_tokens(columns: set[str]) -> dict[str, set[str]]:
        tokens: dict[str, set[str]] = {}
        for column in columns:
            normalized = UpdateAgent._normalize_label(column)
            if not normalized:
                continue
            tokens[column.upper()] = {
                token.lower()
                for token in normalized.split("_")
                if token
            }
        return tokens

    def _generate_reasoning(
        self,
        *,
        ticket_id: str,
        record_id: str,
        applied: list[dict[str, Any]],
        unmatched: list[dict[str, Any]],
        empty: list[dict[str, Any]],
    ) -> str | None:
        if self.llm_client is None:
            return None
        messages = [
            {
                "role": "system",
                "content": (
                    "You summarize CRM updates. Given the applied facts, unmatched facts,"
                    " and empty facts, produce a short human-readable justification."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Ticket: {ticket_id}\nRecord: {record_id}\n"
                    f"Applied facts: {applied}\nUnmatched facts: {unmatched}\nEmpty facts: {empty}"
                ),
            },
        ]
        try:
            response = self.llm_client.generate(messages=messages)
        except Exception:
            return None
        return _extract_text_response(response)


def _extract_text_response(response: Any) -> str | None:  # pragma: no cover - helper mirrors query agent logic
    output = getattr(response, "output", [])
    for item in output:
        content = getattr(item, "content", None)
        if isinstance(content, list):
            for block in content:
                text = getattr(block, "text", "")
                if text:
                    return text.strip()
        else:
            text = getattr(content, "text", "")
            if text:
                return text.strip()
    return None
