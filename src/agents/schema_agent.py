"""Schema agent that designs new fields and migrations for uncovered gaps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class MigrationWriter(Protocol):
    """Outputs SQL migration scripts for schema changes."""

    def write_migration(
        self, *, name: str, statements: list[str]
    ) -> str:  # pragma: no cover - interface
        """Persist a migration file and return its reference path."""


@dataclass(slots=True)
class ColumnProposal:
    name: str
    data_type: str
    nullable: bool
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
        }


@dataclass
class SchemaAgent:
    """Evaluates schema gaps and proposes durable structural updates."""

    migration_writer: MigrationWriter
    table_name: str = "dataset"

    def propose_change(self, *, ticket_id: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        """Return column specifications and migration metadata for review."""

        unknown_fields: dict[str, Any] = evidence_summary.get("unknown_fields", {})
        if not unknown_fields:
            return {
                "ticket_id": ticket_id,
                "columns": [],
                "migration_path": None,
                "migration_statements": [],
                "notes": "No unknown fields supplied",
            }

        proposals: list[ColumnProposal] = []
        statements: list[str] = []
        for raw_name, sample in unknown_fields.items():
            column_name = self._normalize_name(raw_name)
            sql_type = self._infer_sql_type(sample)
            description = self._describe_source(sample)
            proposals.append(
                ColumnProposal(
                    name=column_name,
                    data_type=sql_type,
                    nullable=True,
                    description=description,
                )
            )
            statements.append(
                f'ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS "{column_name}" {sql_type};'
            )

        migration_name = f"ticket_{ticket_id.lower()}"
        migration_path = self.migration_writer.write_migration(
            name=migration_name,
            statements=statements,
        )

        return {
            "ticket_id": ticket_id,
            "columns": [proposal.to_dict() for proposal in proposals],
            "migration_path": migration_path,
            "migration_statements": statements,
        }

    @staticmethod
    def _normalize_name(raw_name: str) -> str:
        return raw_name.strip().lower().replace(" ", "_").replace("-", "_").upper()

    @staticmethod
    def _describe_source(sample: Any) -> str:
        sample_repr = repr(sample)
        max_length = 80
        if len(sample_repr) > max_length:
            sample_repr = sample_repr[: max_length - 3] + "..."
        return f"Inferred from sample value {sample_repr}"

    @staticmethod
    def _infer_sql_type(sample: Any) -> str:
        if isinstance(sample, bool):
            return "BOOLEAN"
        if isinstance(sample, int):
            return "INTEGER"
        if isinstance(sample, float):
            return "NUMERIC"
        if isinstance(sample, (dict, list)):
            return "JSONB"
        return "TEXT"
