"""Schema agent that designs new fields and migrations for uncovered gaps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from src.integrations.openai_agent_sdk import OpenAIAgentAdapter


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
    llm_client: OpenAIAgentAdapter | None = None

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

        proposals = self._generate_proposals(unknown_fields)
        if not proposals:
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

        statements = [
            f'ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS "{proposal.name}" {proposal.data_type};'
            for proposal in proposals
        ]

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

    def _generate_proposals(self, unknown_fields: dict[str, Any]) -> list[ColumnProposal]:
        if self.llm_client is None:
            return []
        messages = [
            {
                "role": "system",
                "content": (
                    "You design database columns. Given unknown field names and example"
                    " values, respond with JSON: [{\"name\":...,\"data_type\":...,\"nullable\":bool,\"description\":...}]."
                ),
            },
            {
                "role": "user",
                "content": str(unknown_fields),
            },
        ]
        try:
            response = self.llm_client.generate(messages=messages)
        except Exception:
            return []
        payload = _extract_schema_json(response)
        proposals: list[ColumnProposal] = []
        for item in payload:
            name = item.get("name")
            data_type = item.get("data_type") or item.get("datatype")
            nullable = item.get("nullable", True)
            description = item.get("description", "")
            if name and data_type:
                proposals.append(
                    ColumnProposal(
                        name=self._normalize_name(name),
                        data_type=str(data_type).upper(),
                        nullable=bool(nullable),
                        description=description,
                    )
                )
        return proposals


def _extract_schema_json(response: Any) -> list[dict[str, Any]]:  # pragma: no cover - helper akin to other parsers
    output = getattr(response, "output", [])
    for item in output:
        content = getattr(item, "content", None)
        if isinstance(content, list):
            for block in content:
                text = getattr(block, "text", "")
                if text:
                    parsed = _safe_load_json(text)
                    if isinstance(parsed, list):
                        return [dict(entry) for entry in parsed if isinstance(entry, dict)]
        else:
            text = getattr(content, "text", "")
            if text:
                parsed = _safe_load_json(text)
                if isinstance(parsed, list):
                    return [dict(entry) for entry in parsed if isinstance(entry, dict)]
    return []


def _safe_load_json(text: str) -> Any:  # pragma: no cover - helper
    import json

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
