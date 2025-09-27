"""OpenAI Responses API-backed search client."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any


def _default_client_factory(api_key: str) -> Any:
    try:
        module = importlib.import_module("openai")
    except ModuleNotFoundError as exc:  # pragma: no cover - handled in tests
        raise ImportError(
            "openai package is required for OpenAIWebSearchClient. Install openai>=1.0"
        ) from exc
    openai_client = getattr(module, "OpenAI", None)
    if openai_client is None:  # pragma: no cover - defensive
        raise ImportError("openai.OpenAI client is not available in installed package")
    return openai_client(api_key=api_key)


@dataclass(slots=True)
class OpenAIWebSearchClient:
    """SearchClient implementation using the OpenAI Responses API."""

    model: str
    api_key: str
    max_results: int = 5
    max_output_tokens: int | None = None
    client_factory: Callable[[str], Any] = field(default=_default_client_factory)
    _client: Any = field(init=False)

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("api_key is required for OpenAIWebSearchClient")
        self._client = self.client_factory(self.api_key)

    def search(self, query: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        limit = limit or self.max_results
        response = self._client.responses.create(
            model=self.model,
            input=query,
            tools=[{"type": "web_search"}],
            max_output_tokens=self.max_output_tokens,
        )
        return self._parse_response(response, limit)

    @staticmethod
    def _parse_response(response: Any, limit: int) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        seen_texts: set[str] = set()
        output = getattr(response, "output", [])
        for item in _ensure_iterable(output):
            item_type = _get_attr(item, "type")
            if item_type != "tool_result":
                results.extend(
                    _results_from_text_blocks(
                        item, limit - len(results), seen_texts
                    )
                )
                if len(results) >= limit:
                    return results[:limit]
                continue
            tool_type = _get_attr(item, "tool_type")
            if tool_type != "web_search":
                continue
            content = _get_attr(item, "content", default=[])
            for entry in _ensure_iterable(content):
                data = _get_attr(entry, "data")
                if isinstance(data, list):
                    for datum in data:
                        if isinstance(datum, dict):
                            results.append(datum)
                            if len(results) >= limit:
                                return results[:limit]
                text = _get_attr(entry, "text")
                if text and _remember_text(text, seen_texts):
                    results.append({"text": text})
                    if len(results) >= limit:
                        return results[:limit]
        if len(results) < limit:
            for text in _iter_output_text(response):
                if not _remember_text(text, seen_texts):
                    continue
                results.append({"text": text})
                if len(results) >= limit:
                    break
        return results[:limit]


def _ensure_iterable(value: Any) -> Iterable[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return value
    return [value]


def _get_attr(obj: Any, name: str, default: Any | None = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _results_from_text_blocks(
    item: Any, remaining: int, seen_texts: set[str]
) -> list[dict[str, Any]]:
    if remaining <= 0:
        return []
    texts: list[str] = []
    content = _get_attr(item, "content", default=[])
    for block in _ensure_iterable(content):
        text = _get_attr(block, "text")
        if text:
            texts.append(text)
        elif isinstance(block, str):
            texts.append(block)
        elif isinstance(block, dict):
            # Some SDK builds return {"type": "output_text", "text": "..."}
            maybe_text = block.get("output_text") if "output_text" in block else None
            if maybe_text:
                texts.extend(_ensure_iterable(maybe_text))
    records: list[dict[str, Any]] = []
    for text in texts:
        if not _remember_text(text, seen_texts):
            continue
        records.append({"text": text})
        if len(records) >= remaining:
            break
    return records


def _iter_output_text(response: Any) -> Iterable[str]:
    output_text = getattr(response, "output_text", None)
    if output_text:
        for text in _ensure_iterable(output_text):
            if isinstance(text, str) and text.strip():
                yield text
    # Some client builds expose a .response output with consolidated text
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        yield text


def _remember_text(text: str, seen_texts: set[str]) -> bool:
    normalized = text.strip()
    if not normalized:
        return False
    if normalized in seen_texts:
        return False
    seen_texts.add(normalized)
    return True
