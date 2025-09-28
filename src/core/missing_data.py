"""Missing data flagger implementations used by the orchestration layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.agents.query_agent import MissingDataFlagger
from src.core.logging_utils import resolve_log_path, utc_now_iso


@dataclass(slots=True)
class JSONLMissingDataFlagger(MissingDataFlagger):
    """Persists missing-data events to JSONL files under assets/scrapes."""

    base_dir: Path

    def flag_missing(self, ticket_id: str, question: str, facts: dict[str, Any]) -> None:  # type: ignore[override]
        payload = {
            "ticket_id": ticket_id,
            "question": question,
            "facts": facts,
            "timestamp": utc_now_iso(),
        }
        target = resolve_log_path(self.base_dir, ticket_id, payload["timestamp"])
        with target.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
