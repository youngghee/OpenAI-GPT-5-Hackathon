"""Tests for the update agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.agents.update_agent import CRMClient, SchemaEscalator, UpdateAgent


@dataclass
class _CRMStub(CRMClient):
    updates: list[dict[str, Any]] = field(default_factory=list)

    def update_record(self, record_id: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        self.updates.append({"record_id": record_id, "payload": payload})


@dataclass
class _SchemaEscalatorStub(SchemaEscalator):
    escalations: list[dict[str, Any]] = field(default_factory=list)

    def escalate(self, ticket_id: str, rationale: dict[str, Any]) -> None:  # type: ignore[override]
        self.escalations.append({"ticket_id": ticket_id, "rationale": rationale})


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


def test_apply_enrichment_updates_known_fields() -> None:
    crm = _CRMStub()
    escalator = _SchemaEscalatorStub()
    agent = UpdateAgent(
        crm_client=crm, schema_escalator=escalator, allowed_fields={"BUSINESS_NAME"}
    )

    summary = agent.apply_enrichment(
        ticket_id="T-1",
        record_id="row-1",
        enriched_fields={"business_name": "New Name"},
    )

    assert crm.updates == [{"record_id": "row-1", "payload": {"BUSINESS_NAME": "New Name"}}]
    assert summary["status"] == "updated"
    assert summary["applied_fields"] == ["BUSINESS_NAME"]
    assert escalator.escalations == []


def test_apply_enrichment_escalates_unknown_fields() -> None:
    crm = _CRMStub()
    escalator = _SchemaEscalatorStub()
    agent = UpdateAgent(
        crm_client=crm, schema_escalator=escalator, allowed_fields={"BUSINESS_NAME"}
    )

    summary = agent.apply_enrichment(
        ticket_id="T-2",
        record_id="row-1",
        enriched_fields={"new_metric": 42},
    )

    assert summary["status"] == "skipped"
    assert escalator.escalations and escalator.escalations[0]["rationale"]["unknown_fields"] == {
        "NEW_METRIC": 42
    }
    assert crm.updates == []


def test_apply_enrichment_generates_reasoning_with_llm() -> None:
    crm = _CRMStub()
    escalator = _SchemaEscalatorStub()
    llm = _LLMStub("Applied BUSINESS_NAME; no issues detected.")
    agent = UpdateAgent(
        crm_client=crm,
        schema_escalator=escalator,
        allowed_fields={"BUSINESS_NAME"},
        llm_client=llm,
    )

    summary = agent.apply_enrichment(
        ticket_id="T-llm",
        record_id="row-1",
        enriched_fields={"business_name": "New Name"},
    )

    assert "reasoning" in summary
    assert "BUSINESS_NAME" in summary["reasoning"]
    assert llm.calls


def test_apply_enrichment_ignores_empty_values() -> None:
    crm = _CRMStub()
    escalator = _SchemaEscalatorStub()
    agent = UpdateAgent(crm_client=crm, schema_escalator=escalator, allowed_fields=None)

    summary = agent.apply_enrichment(
        ticket_id="T-3",
        record_id="row-1",
        enriched_fields={"business_name": "   "},
    )

    assert summary["status"] == "skipped"
    assert escalator.escalations and "empty_fields" in escalator.escalations[0]["rationale"]
    assert crm.updates == []
