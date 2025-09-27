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
        output = getattr(response, "output", [])
        for item in _ensure_iterable(output):
            item_type = _get_attr(item, "type")
            if item_type != "tool_result":
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
                if text:
                    results.append({"text": text})
                    if len(results) >= limit:
                        return results[:limit]
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
