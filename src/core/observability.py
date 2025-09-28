"""JSONL-backed observability helpers for agent workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from src.core.logging_utils import resolve_log_path, utc_now_iso


class QueryObservationSink(Protocol):
    """Records lifecycle events emitted by the query agent."""

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # pragma: no cover - interface
        ...


class ScraperObservationSink(Protocol):
    """Records lifecycle events emitted by the scraper agent."""

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # pragma: no cover - interface
        ...


def _write_jsonl(base_dir: Path, ticket_id: str, payload: dict[str, Any]) -> None:
    target = resolve_log_path(
        base_dir=base_dir,
        ticket_id=ticket_id,
        timestamp=payload.get("timestamp") if isinstance(payload, dict) else None,
    )
    with target.open("a", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
        handle.write("\n")


def _build_event(event: str, payload: dict[str, Any]) -> dict[str, Any]:
    enriched = {key: value for key, value in payload.items() if value is not None}
    enriched.setdefault("event", event)
    enriched.setdefault("timestamp", utc_now_iso())
    return enriched


@dataclass(slots=True)
class JSONLQueryLogger(QueryObservationSink):
    """Persists query agent events under a dedicated logs directory."""

    base_dir: Path

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        _write_jsonl(self.base_dir, ticket_id, _build_event(event, payload))


@dataclass(slots=True)
class JSONLScraperLogger(ScraperObservationSink):
    """Persists scraper agent events under a dedicated logs directory."""

    base_dir: Path

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        _write_jsonl(self.base_dir, ticket_id, _build_event(event, payload))
