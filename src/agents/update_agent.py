"""Update agent that reconciles enriched facts back into the CRM."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class CRMClient(Protocol):
    """API surface for interacting with the source-of-truth system."""

    def update_record(self, record_id: str, payload: dict[str, str]) -> None:  # pragma: no cover - interface
        """Persist deterministic updates for a CRM record."""


class SchemaEscalator(Protocol):
    """Raised when enriched data does not map onto existing columns."""

    def escalate(self, ticket_id: str, rationale: str) -> None:  # pragma: no cover - interface
        """Forward schema-change requests downstream."""


@dataclass
class UpdateAgent:
    """Performs reconciliation after the query agent produces a satisfactory answer."""

    crm_client: CRMClient
    schema_escalator: SchemaEscalator

    def apply_enrichment(self, *, ticket_id: str, record_id: str, enriched_fields: dict[str, str]) -> None:
        """Attempt to persist new facts and escalate when schema gaps occur."""

        # TODO: detect unmapped fields and trigger schema escalation.
        raise NotImplementedError("UpdateAgent.apply_enrichment is pending implementation")
