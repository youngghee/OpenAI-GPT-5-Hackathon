"""Evidence sinks used by agents to persist gathered facts."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class EvidenceSink(Protocol):
    """Destination that accepts normalized evidence payloads."""

    def append(
        self, ticket_id: str, payload: dict[str, Any]
    ) -> None:  # pragma: no cover - interface
        ...

    def bulk_append(
        self, ticket_id: str, payloads: Iterable[dict[str, Any]]
    ) -> None:  # pragma: no cover - interface
        ...


@dataclass(slots=True)
class JSONLEvidenceSink(EvidenceSink):
    """Persists evidence as JSON lines under a base directory."""

    base_dir: Path

    def append(
        self, ticket_id: str, payload: dict[str, Any]
    ) -> None:  # pragma: no cover - exercised via bulk
        self.base_dir.mkdir(parents=True, exist_ok=True)
        target = self.base_dir / f"{ticket_id}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")

    def bulk_append(self, ticket_id: str, payloads: Iterable[dict[str, Any]]) -> None:
        for payload in payloads:
            self.append(ticket_id, payload)
