"""Command-line entry point for running multi-agent simulations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class ScenarioLoader(Protocol):
    """Provides iterative scenarios for the orchestrator to execute."""

    def load(self, profile: str) -> list[dict[str, str]]:  # pragma: no cover - interface
        """Return scenario definitions for the requested profile."""


@dataclass
class Runner:
    """Coordinates the end-to-end agent workflow for a simulation run."""

    scenario_loader: ScenarioLoader

    def execute(self, profile: str) -> None:
        """Run all scenarios defined for the supplied profile."""

        # TODO: orchestrate query, scraper, update, and schema agents per scenario.
        raise NotImplementedError("Runner.execute is pending implementation")


def main() -> None:
    """Placeholder CLI entry point for `python -m src.core.runner`."""

    # TODO: parse CLI arguments, instantiate dependencies, and invoke Runner.
    raise NotImplementedError("Runner.main is pending implementation")


if __name__ == "__main__":
    main()
