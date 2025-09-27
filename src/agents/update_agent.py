"""Update agent that reconciles enriched facts back into the CRM."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

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

    def apply_enrichment(
        self, *, ticket_id: str, record_id: str, enriched_fields: dict[str, Any]
    ) -> dict[str, Any]:
        """Attempt to persist new facts and escalate when schema gaps occur."""

        normalized = {
            self._normalize_field(field): value for field, value in enriched_fields.items()
        }

        allowed = {field.upper() for field in self.allowed_fields} if self.allowed_fields else None

        applicable: dict[str, Any] = {}
        unknown: dict[str, Any] = {}
        empty_fields: list[str] = []

        for field_name, value in normalized.items():
            if not self._has_value(value):
                empty_fields.append(field_name)
                continue

            if allowed is None or field_name in allowed:
                applicable[field_name] = value
            else:
                unknown[field_name] = value

        summary: dict[str, Any] = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "attempted_fields": sorted(normalized.keys()),
        }

        if applicable:
            self.crm_client.update_record(record_id, applicable)
            summary["status"] = "updated"
            summary["applied_fields"] = sorted(applicable.keys())
        else:
            summary["status"] = "skipped"

        escalation_payload: dict[str, Any] = {}
        if unknown:
            escalation_payload["unknown_fields"] = unknown
        if empty_fields:
            escalation_payload["empty_fields"] = empty_fields

        if escalation_payload:
            self.schema_escalator.escalate(ticket_id, escalation_payload)
            summary["escalated"] = escalation_payload

        reasoning = self._generate_reasoning(
            ticket_id=ticket_id,
            record_id=record_id,
            applied=applicable,
            unknown=unknown,
            empty_fields=empty_fields,
        )
        if reasoning:
            summary["reasoning"] = reasoning

        return summary

    @staticmethod
    def _normalize_field(field: str) -> str:
        return field.strip().upper()

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, dict, set, tuple)):
            return len(value) > 0
        return True

    def _generate_reasoning(
        self,
        *,
        ticket_id: str,
        record_id: str,
        applied: dict[str, Any],
        unknown: dict[str, Any],
        empty_fields: list[str],
    ) -> str | None:
        if self.llm_client is None:
            return None
        messages = [
            {
                "role": "system",
                "content": (
                    "You summarize CRM updates. Given the applied updates, unknown fields,"
                    " and empty fields, produce a short human-readable justification."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Ticket: {ticket_id}\nRecord: {record_id}\n"
                    f"Applied: {applied}\nUnknown: {unknown}\nEmpty: {empty_fields}"
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
