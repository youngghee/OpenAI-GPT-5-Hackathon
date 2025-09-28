"""Schema escalation utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.agents.update_agent import SchemaEscalator
from src.core.logging_utils import resolve_log_path, utc_now_iso


@dataclass(slots=True)
class JSONLSchemaEscalator(SchemaEscalator):
    """Persists schema escalation requests to JSONL files."""

    base_dir: Path

    def escalate(self, ticket_id: str, rationale: dict[str, Any]) -> None:  # type: ignore[override]
        payload = {
            "ticket_id": ticket_id,
            "rationale": rationale,
            "timestamp": utc_now_iso(),
        }
        target = resolve_log_path(self.base_dir, ticket_id, payload["timestamp"])
        with target.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
