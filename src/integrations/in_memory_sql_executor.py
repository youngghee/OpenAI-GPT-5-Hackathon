"""Lightweight, in-memory SQL executor stub for theoretical workflows.

This executor does not parse SQL or connect to a live database. Instead, it
returns canned responses that mimic results a real query engine might produce.
Use it inside tests or early prototypes while the production Codex integration
is still in development.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class InMemorySQLExecutor:
    """Simple mapping-based executor that satisfies the `SQLExecutor` protocol."""

    canned_results: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def run(self, statement: str) -> list[dict[str, Any]]:
        """Return the canned result for the supplied SQL statement."""

        return list(self.canned_results.get(statement, []))

    def prime(self, statement: str, rows: list[dict[str, Any]]) -> None:
        """Register a canned response for a future `run` call."""

        self.canned_results[statement] = list(rows)
