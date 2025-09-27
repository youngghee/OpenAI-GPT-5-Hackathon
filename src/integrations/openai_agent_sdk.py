"""Adapter for interacting with the OpenAI Agents SDK with fallback support."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Iterable

from src.integrations.openai_models import GPTResponseClient, OpenAIClientFactory


@dataclass(slots=True)
class OpenAIAgentAdapter:
    """Wraps the Agents SDK while gracefully falling back to the Responses API."""

    model: str
    fallback: GPTResponseClient
    factory: OpenAIClientFactory = field(default_factory=OpenAIClientFactory)
    agents_module: Any | None = field(init=False, default=None)
    client: Any | None = field(init=False, default=None)
    agent_id: str | None = field(init=False, default=None)
    session_id: str | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        try:
            self.agents_module = importlib.import_module("openai.agents")
        except ModuleNotFoundError:
            self.agents_module = None
            return
        try:
            self._initialise_agent()
        except Exception:
            # If anything goes wrong during initialisation, fall back silently.
            self.agents_module = None
            self.client = None
            self.agent_id = None
            self.session_id = None

    # ------------------------------------------------------------------
    # Public API mirroring the fallback client
    # ------------------------------------------------------------------

    def generate(
        self,
        *,
        messages: Iterable[dict[str, str]],
        max_output_tokens: int | None = None,
        tools: list[dict[str, Any]] | None = None,
    ) -> Any:
        if not self._is_agent_ready():
            return self.fallback.generate(
                messages=messages, max_output_tokens=max_output_tokens, tools=tools
            )
        try:
            response = self.client.agents.responses.create(  # type: ignore[operator]
                agent=self.agent_id,
                session=self.session_id,
                input=self._format_messages(messages),
                max_output_tokens=max_output_tokens,
                tools=tools,
            )
            return response
        except Exception:
            return self.fallback.generate(
                messages=messages, max_output_tokens=max_output_tokens, tools=tools
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _initialise_agent(self) -> None:
        client = self.factory.create()
        agent = client.agents.create(model=self.model)
        session = client.agents.sessions.create(agent=agent.id)
        self.client = client
        self.agent_id = agent.id
        self.session_id = session.id

    def _is_agent_ready(self) -> bool:
        return bool(self.client and self.agent_id and self.session_id)

    @staticmethod
    def _format_messages(messages: Iterable[dict[str, str]]) -> list[dict[str, Any]]:
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
