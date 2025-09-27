"""Command-line entry point for running multi-agent simulations."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml

from src.agents.query_agent import QueryAgent
from src.core.config import load_settings
from src.core.dependencies import RunnerDependencies, build_dependencies


class ScenarioLoader(Protocol):
    """Provides iterative scenarios for the orchestrator to execute."""

    def load(self, profile: str) -> list[dict[str, Any]]:  # pragma: no cover - interface
        """Return scenario definitions for the requested profile."""


@dataclass(slots=True)
class YamlScenarioLoader(ScenarioLoader):
    """Loads scenarios from YAML files located under a base directory."""

    base_dir: Path

    def load(self, profile: str) -> list[dict[str, Any]]:
        target = self.base_dir / f"{profile}.yaml"
        if not target.exists():
            return []
        with target.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or []
        if not isinstance(payload, list):
            raise ValueError("Scenario file must contain a top-level list")
        scenarios: list[dict[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, dict):
                raise ValueError("Scenario entries must be mappings")
            scenarios.append({str(key): value for key, value in entry.items()})
        return scenarios


@dataclass
class Runner:
    """Coordinates the end-to-end agent workflow for a simulation run."""

    scenario_loader: ScenarioLoader
    dependencies: RunnerDependencies | None = None

    def execute(self, profile: str) -> list[dict[str, Any]]:
        """Run all scenarios defined for the supplied profile."""

        if self.dependencies is None:
            raise ValueError("Runner dependencies must be provided")
        if self.dependencies.sql_executor is None:
            raise ValueError("SQL executor dependency is required")
        if self.dependencies.missing_data_flagger is None:
            raise ValueError("Missing data flagger dependency is required")

        scenarios = self.scenario_loader.load(profile)
        results: list[dict[str, Any]] = []

        for scenario in scenarios:
            ticket_id = str(scenario.get("ticket_id"))
            question = str(scenario.get("question"))
            record_id = str(scenario.get("record_id"))
            primary_key = str(scenario.get("primary_key_column", "BRIZO_ID"))
            table_name = str(scenario.get("table_name", "dataset"))

            agent = QueryAgent(
                sql_executor=self.dependencies.sql_executor,
                missing_data_flagger=self.dependencies.missing_data_flagger,
                primary_key_column=primary_key,
                table_name=table_name,
            )

            result = agent.answer_question(
                ticket_id=ticket_id,
                question=question,
                record_id=record_id,
            )
            results.append(result)

        return results


def main() -> None:
    """CLI entry point for running query scenarios."""

    parser = argparse.ArgumentParser(description="Run query agent scenarios")
    parser.add_argument("--config", default="configs/dev.yaml", help="Path to the YAML config file")
    parser.add_argument("--profile", default="dev", help="Scenario profile to execute")
    parser.add_argument(
        "--scenarios",
        default="assets/scenarios",
        help="Directory containing scenario YAML files",
    )
    args = parser.parse_args()

    settings = load_settings(args.config)
    dependencies = build_dependencies(settings)
    loader = YamlScenarioLoader(base_dir=Path(args.scenarios))
    runner = Runner(scenario_loader=loader, dependencies=dependencies)

    results = runner.execute(profile=args.profile)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
