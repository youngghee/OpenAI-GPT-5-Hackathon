"""Adapter for the OpenAI Agents SDK with graceful fallback."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Sequence

from src.integrations.openai_models import GPTResponseClient, OpenAIClientFactory


@dataclass(slots=True)
class OpenAIAgentAdapter:
    """Uses the Agents SDK when available, otherwise falls back to Responses API."""

    model: str
    fallback: GPTResponseClient
    factory: OpenAIClientFactory = field(default_factory=OpenAIClientFactory)

    def __post_init__(self) -> None:
        try:
            self._agents_module = importlib.import_module("openai.agents")
        except ModuleNotFoundError:
            self._agents_module = None
        self._client: Any | None = None
        self._agent_id: str | None = None
        self._session_id: str | None = None
        if self._agents_module is not None:
            try:
                self._initialise_agent()
            except Exception:
                # If the SDK is present but fails to initialise, fall back silently.
                self._agents_module = None

    def generate(
        self,
        *,
        messages: Sequence[dict[str, str]],
        max_output_tokens: int | None = None,
        tools: list[dict[str, Any]] | None = None,
    ) -> Any:
        if self._agents_module is None:
            return self.fallback.generate(
                messages=messages, max_output_tokens=max_output_tokens, tools=tools
            )
        try:
            inputs = self._format_messages(messages)
            response = self._client.agents.responses.create(  # type: ignore[operator]
                agent=self._agent_id,
                session=self._session_id,
                input=inputs,
                max_output_tokens=max_output_tokens,
                tools=tools,
            )
            return response
        except Exception:
            # Drop to fallback path on any SDK error.
            return self.fallback.generate(
                messages=messages, max_output_tokens=max_output_tokens, tools=tools
            )

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    def _initialise_agent(self) -> None:
        client = self.factory.create()
        agent = client.agents.create(model=self.model)
        session = client.agents.sessions.create(agent=agent.id)
        self._client = client
        self._agent_id = agent.id
        self._session_id = session.id

    @staticmethod
    def _format_messages(messages: Sequence[dict[str, str]]) -> list[dict[str, Any]]:
        formatted: list[dict[str, Any]] = []
        for message in messages:
            role = message.get("role", "user")
            content = message.get("content", "")
            formatted.append(
                {
                    "role": role,
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": content,
                        }
                    ],
                }
            )
        return formatted
