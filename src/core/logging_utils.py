"""Shared helpers for timestamped JSONL logging."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Tuple


_FILENAME_CACHE: Dict[Tuple[str, str], Path] = {}


def make_timestamp_slug(raw: str | None = None) -> str:
    """Return a sortable timestamp slug (UTC) suitable for filenames."""

    candidate = (raw or "").strip()
    if candidate:
        sanitized = candidate[:-1] if candidate.endswith("Z") else candidate
        try:
            parsed = datetime.fromisoformat(sanitized)
        except ValueError:
            parsed = None
    else:
        parsed = None

    if parsed is None:
        parsed = datetime.now(UTC)

    return parsed.strftime("%Y%m%dT%H%M%S%f")[:-3]


def sanitize_ticket_id(ticket_id: str) -> str:
    """Sanitize *ticket_id* so it can be embedded in filenames."""

    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", ticket_id.strip())
    return cleaned or "ticket"


def resolve_log_path(base_dir: Path, ticket_id: str, timestamp: str | None = None) -> Path:
    """Return a cached, timestamp-prefixed path for the given ticket."""

    normalized_base = str(base_dir.expanduser().resolve())
    key = (normalized_base, ticket_id)
    if key in _FILENAME_CACHE:
        return _FILENAME_CACHE[key]

    slug = make_timestamp_slug(timestamp)
    safe_ticket = sanitize_ticket_id(ticket_id)
    filename = f"{slug}-{safe_ticket}.jsonl"
    target = Path(normalized_base) / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    _FILENAME_CACHE[key] = target
    return target


def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 with millisecond precision."""

    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
