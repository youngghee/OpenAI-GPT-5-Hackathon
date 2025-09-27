"""Helpers for working with CRM record dictionaries."""

from __future__ import annotations

from typing import Any, Iterable

DEFAULT_CONTEXT_COLUMNS: tuple[str, ...] = (
    "BUSINESS_NAME",
    "ALTERNATE_NAME",
    "PARENT_NAME",
    "CHAIN_NAME",
    "LOCATION_CITY",
    "LOCATION_STATE_CODE",
    "LOCATION_COUNTRY",
)

_MISSING_TEXT = {"na", "n/a", "none", "null", "nan"}


def build_record_context(
    row: dict[str, Any] | None,
    columns: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Return a distilled view of *row* with helpful business context."""

    if not row:
        return {}

    selected = list(columns) if columns is not None else list(DEFAULT_CONTEXT_COLUMNS)
    context: dict[str, Any] = {}
    for column in selected:
        value = row.get(column)
        if value is None:
            continue
        if isinstance(value, str) and _is_missing_text(value):
            continue
        context[column] = value
    return context


def extract_candidate_urls(
    row: dict[str, Any] | None,
    candidate_fields: Iterable[str] | None = None,
) -> list[str]:
    """Return normalized URL candidates derived from *row*."""

    if not row:
        return []

    urls: list[str] = []
    seen: set[str] = set()
    columns = list(candidate_fields) if candidate_fields is not None else list(row.keys())
    for column in columns:
        value = row.get(column)
        if not value:
            continue
        normalized = normalize_url(str(value))
        if not normalized or normalized in seen:
            continue
        urls.append(normalized)
        seen.add(normalized)
    return urls


def normalize_url(value: str) -> str | None:
    """Normalize *value* into a navigable URL when possible."""

    text = value.strip()
    if not looks_like_url(text):
        return None

    lowered = text.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        base = text
    elif lowered.startswith("www."):
        base = f"https://{text}"
    else:
        base = f"https://{text}"
    return base.rstrip("/")


def looks_like_url(value: str) -> bool:
    """Heuristic check for whether *value* resembles a URL or hostname."""

    text = value.strip()
    if not text:
        return False

    lowered = text.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return True
    if lowered.startswith("www."):
        return True
    if " " in lowered:
        return False
    if "." not in lowered:
        return False

    host_candidate = lowered.split("/")[0]
    if host_candidate.count(".") >= 1 and not host_candidate.endswith(".") and any(
        ch.isalpha() for ch in host_candidate
    ):
        return True
    return False


def _is_missing_text(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return True
    return stripped.lower() in _MISSING_TEXT


__all__ = [
    "DEFAULT_CONTEXT_COLUMNS",
    "build_record_context",
    "extract_candidate_urls",
    "looks_like_url",
    "normalize_url",
]
