"""Schema escalation utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.agents.update_agent import SchemaEscalator


@dataclass(slots=True)
class JSONLSchemaEscalator(SchemaEscalator):
    """Persists schema escalation requests to JSONL files."""

    base_dir: Path

    def escalate(self, ticket_id: str, rationale: dict[str, Any]) -> None:  # type: ignore[override]
        self.base_dir.mkdir(parents=True, exist_ok=True)
        target = self.base_dir / f"{ticket_id}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            json.dump({"ticket_id": ticket_id, "rationale": rationale}, handle, ensure_ascii=False)
            handle.write("\n")
