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
        _validate_dependencies(self.dependencies)

        scenarios = self.scenario_loader.load(profile)
        return [self._run_scenario(scenario) for scenario in scenarios]

    def _run_scenario(self, scenario: dict[str, Any]) -> dict[str, Any]:
        assert self.dependencies is not None  # for mypy; guarded in execute
        return run_scenario(self.dependencies, scenario)

    @staticmethod
    def _resolve_enrichment_payload(
        result: dict[str, Any], scenario: dict[str, Any]
    ) -> dict[str, Any] | None:
        scenario_enrichment = scenario.get("enriched_fields")
        if isinstance(scenario_enrichment, dict):
            return scenario_enrichment
        if result.get("status") == "answered" and "answers" in result:
            answers = result["answers"]
            if isinstance(answers, dict):
                return answers
        return None


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


def run_scenario(dependencies: RunnerDependencies, scenario: dict[str, Any]) -> dict[str, Any]:
    """Execute a single scenario using the provided *dependencies*."""

    _validate_dependencies(dependencies)

    ticket_id = str(scenario.get("ticket_id"))
    question = str(scenario.get("question"))
    record_id = str(scenario.get("record_id"))
    primary_key = str(scenario.get("primary_key_column", "BRIZO_ID"))
    table_name = str(scenario.get("table_name", "dataset"))

    agent = QueryAgent(
        sql_executor=dependencies.sql_executor,
        missing_data_flagger=dependencies.missing_data_flagger,
        primary_key_column=primary_key,
        table_name=table_name,
        logger=dependencies.query_logger,
    )

    result = agent.answer_question(
        ticket_id=ticket_id,
        question=question,
        record_id=record_id,
    )

    if result.get("status") != "answered":
        _augment_with_scraper(dependencies, ticket_id, question, result)

    enrichment_payload = Runner._resolve_enrichment_payload(result, scenario)
    schema_proposal: dict[str, Any] | None = None

    if enrichment_payload:
        update_summary = dependencies.update_agent.apply_enrichment(
            ticket_id=ticket_id,
            record_id=record_id,
            enriched_fields=enrichment_payload,
        )
        result["update"] = update_summary

        escalated = update_summary.get("escalated") if isinstance(update_summary, dict) else None
        if escalated:
            schema_proposal = dependencies.schema_agent.propose_change(
                ticket_id=ticket_id,
                evidence_summary=escalated,
            )

    if schema_proposal:
        result["schema_proposal"] = schema_proposal

    return result


def _augment_with_scraper(
    dependencies: RunnerDependencies, ticket_id: str, question: str, result: dict[str, Any]
) -> None:
    assert dependencies.scraper_agent is not None
    missing_facts: dict[str, Any] = {"status": result.get("status")}
    if "missing_columns" in result:
        missing_facts["missing_columns"] = result["missing_columns"]
    outcome = dependencies.scraper_agent.execute_plan(
        ticket_id=ticket_id,
        question=question,
        missing_facts=missing_facts,
    )
    if outcome.tasks:
        result["scraper_tasks"] = [task.to_dict() for task in outcome.tasks]
    if outcome.findings:
        result["scraper_findings"] = len(outcome.findings)


def _validate_dependencies(dependencies: RunnerDependencies) -> None:
    if dependencies.sql_executor is None:
        raise ValueError("SQL executor dependency is required")
    if dependencies.missing_data_flagger is None:
        raise ValueError("Missing data flagger dependency is required")
    if dependencies.scraper_agent is None:
        raise ValueError("Scraper agent dependency is required")
    if dependencies.update_agent is None:
        raise ValueError("Update agent dependency is required")
    if dependencies.schema_agent is None:
        raise ValueError("Schema agent dependency is required")
