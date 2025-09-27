"""Tests for the OpenAI web search client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from src.integrations.openai_search import OpenAIWebSearchClient


@dataclass
class _ResponseObject:
    output: list[Any]
    output_text: list[str] | None = None


@dataclass
class _ToolContent:
    data: list[dict[str, Any]] | None = None
    text: str | None = None


@dataclass
class _ToolResult:
    type: str
    tool_type: str
    content: list[Any]


@dataclass
class _MessageBlock:
    text: str | None = None


@dataclass
class _Message:
    type: str
    content: list[Any]


class _ResponsesWrapper:
    def __init__(self, outer: _OpenAIClientStub) -> None:
        self.outer = outer

    def create(self, **kwargs: Any) -> _ResponseObject:  # type: ignore[override]
        self.outer.calls.append(kwargs)
        return self.outer._responses


class _OpenAIClientStub:
    def __init__(self, responses: _ResponseObject) -> None:
        self._responses = responses
        self.calls: list[dict[str, Any]] = []
        self.responses = _ResponsesWrapper(self)


def _client_factory(_: str) -> _OpenAIClientStub:
    tool_result = _ToolResult(
        type="tool_result",
        tool_type="web_search",
        content=[
            _ToolContent(
                data=[{"title": "Example", "url": "https://example.com", "snippet": "Sample"}]
            )
        ],
    )
    response = _ResponseObject(output=[tool_result])
    return _OpenAIClientStub(responses=response)


def test_openai_web_search_client_parses_tool_results() -> None:
    client_stub = _client_factory("dummy")

    client = OpenAIWebSearchClient(
        model="gpt-4.1-mini",
        api_key="abc",
        client_factory=lambda _: client_stub,
    )

    results = client.search("test query")

    assert results == [{"title": "Example", "url": "https://example.com", "snippet": "Sample"}]
    assert client_stub.calls[0]["model"] == "gpt-4.1-mini"


def test_openai_web_search_client_handles_missing_tool_results() -> None:
    response = _ResponseObject(output=[])
    client_stub = _OpenAIClientStub(responses=response)
    client = OpenAIWebSearchClient(
        model="gpt-4.1-mini",
        api_key="abc",
        client_factory=lambda _: client_stub,
    )

    assert client.search("query") == []


def test_openai_web_search_client_falls_back_to_text_output() -> None:
    response = _ResponseObject(
        output=[
            _Message(type="message", content=[_MessageBlock(text="Chipotle employs ~110,000 people.")])
        ],
        output_text=["Chipotle employs around 110,000 people."],
    )
    client_stub = _OpenAIClientStub(responses=response)
    client = OpenAIWebSearchClient(
        model="gpt-4.1-mini",
        api_key="abc",
        client_factory=lambda _: client_stub,
        max_results=3,
    )

    results = client.search("query")

    assert results[0]["text"].startswith("Chipotle employs")
    assert {record["text"] for record in results} == {
        "Chipotle employs ~110,000 people.",
        "Chipotle employs around 110,000 people.",
    }


def test_openai_web_search_client_deduplicates_identical_text() -> None:
    text = "Chipotle employs ~110,000 people."
    response = _ResponseObject(
        output=[_Message(type="message", content=[_MessageBlock(text=text)])],
        output_text=[text],
    )
    client_stub = _OpenAIClientStub(responses=response)
    client = OpenAIWebSearchClient(
        model="gpt-4.1-mini",
        api_key="abc",
        client_factory=lambda _: client_stub,
        max_results=3,
    )

    results = client.search("query")

    assert len(results) == 1
    assert results[0]["text"] == text


def test_client_requires_api_key() -> None:
    with pytest.raises(ValueError):
        OpenAIWebSearchClient(model="gpt-4.1-mini", api_key="")
