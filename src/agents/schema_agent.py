"""Schema agent that designs new fields and migrations for uncovered gaps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class MigrationWriter(Protocol):
    """Outputs SQL migration scripts for schema changes."""

    def write_migration(self, *, name: str, statements: list[str]) -> str:  # pragma: no cover - interface
        """Persist a migration file and return its reference path."""


@dataclass
class SchemaAgent:
    """Evaluates schema gaps and proposes durable structural updates."""

    migration_writer: MigrationWriter

    def propose_change(self, *, ticket_id: str, evidence_summary: str) -> dict[str, str]:
        """Return column specifications and migration metadata for review."""

        # TODO: inspect evidence, generate column definitions, and craft migrations.
        raise NotImplementedError("SchemaAgent.propose_change is pending implementation")
