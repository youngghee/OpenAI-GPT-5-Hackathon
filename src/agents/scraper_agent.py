"""Scraper agent responsible for gathering external context when data is missing."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol


class SearchClient(Protocol):
    """Interface for running web searches or API lookups."""

    def search(self, query: str) -> list[str]:  # pragma: no cover - interface
        """Return candidate URLs or document identifiers for a query."""


class EvidenceSink(Protocol):
    """Destinations that persist gathered evidence alongside provenance."""

    def append(
        self, ticket_id: str, payload: dict[str, str]
    ) -> None:  # pragma: no cover - interface
        """Store enriched facts for later reconciliation."""


@dataclass
class ScraperAgent:
    """Drafts research plans, manages subagents, and collates findings."""

    search_client: SearchClient
    evidence_sink: EvidenceSink

    def plan_research(self, question: str, missing_facts: Iterable[str]) -> list[str]:
        """Return search directives for subagents based on identified gaps."""

        # TODO: convert missing facts into focused search prompts.
        raise NotImplementedError("ScraperAgent.plan_research is pending implementation")

    def aggregate(self, ticket_id: str, findings: Iterable[dict[str, str]]) -> None:
        """Persist normalized evidence produced by scraper subagents."""

        # TODO: validate findings, deduplicate, and write to assets/scrapes.
        raise NotImplementedError("ScraperAgent.aggregate is pending implementation")
