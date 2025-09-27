"""Missing data flagger implementations used by the orchestration layer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.agents.query_agent import MissingDataFlagger


@dataclass(slots=True)
class JSONLMissingDataFlagger(MissingDataFlagger):
    """Persists missing-data events to JSONL files under assets/scrapes."""

    base_dir: Path

    def flag_missing(self, ticket_id: str, question: str, facts: dict[str, Any]) -> None:  # type: ignore[override]
        self.base_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "ticket_id": ticket_id,
            "question": question,
            "facts": facts,
        }
        target = self.base_dir / f"{ticket_id}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
