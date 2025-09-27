"""Shared OpenAI client utilities for agent workflows."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Sequence


class OpenAIError(RuntimeError):
    """Raised when the OpenAI client cannot be initialised or invoked."""


def _import_openai() -> Any:
    try:
        from openai import OpenAI  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        raise OpenAIError(
            "openai package is required. Install openai>=1.0 to enable GPT-5 integration."
        ) from exc
    return OpenAI


@dataclass(slots=True)
class OpenAIClientFactory:
    """Creates OpenAI client instances with shared configuration."""

    api_key_env: str = "OPENAI_API_KEY"

    def create(self) -> Any:
        OpenAI = _import_openai()
        api_key = os.getenv(self.api_key_env)
        if not api_key:
            raise OpenAIError(
                f"Environment variable '{self.api_key_env}' must be set for GPT-5 integration"
            )
        return OpenAI(api_key=api_key)


@dataclass(slots=True)
class GPTResponseClient:
    """Thin wrapper around the OpenAI Responses API."""

    model: str
    client_factory: OpenAIClientFactory = field(default_factory=OpenAIClientFactory)
    _client: Any | None = None

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = self.client_factory.create()
        return self._client

    def generate(
        self,
        *,
        messages: Sequence[dict[str, str]],
        max_output_tokens: int | None = None,
        tools: list[dict[str, Any]] | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "model": self.model,
            "input": [
                {
                    "role": message.get("role", "user"),
                    "content": message.get("content", ""),
                }
                for message in messages
            ],
        }
        if max_output_tokens is not None:
            payload["max_output_tokens"] = max_output_tokens
        if tools:
            payload["tools"] = tools
        return self.client.responses.create(**payload)
