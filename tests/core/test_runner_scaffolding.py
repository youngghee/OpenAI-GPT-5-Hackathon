"""Runner integration tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.agents.query_agent import MissingDataFlagger, SQLExecutor
from src.agents.scraper_agent import ScrapeOutcome, SearchTask
from src.core.dependencies import RunnerDependencies
from src.core.runner import Runner, YamlScenarioLoader


@dataclass
class _ScenarioLoaderStub:
    scenarios: list[dict[str, Any]]

    def load(self, profile: str) -> list[dict[str, Any]]:  # type: ignore[override]
        return self.scenarios


@dataclass
class _SQLExecutorStub(SQLExecutor):
    dataset: dict[str, dict[str, Any]]
    statements: list[str] = field(default_factory=list)

    def run(self, statement: str) -> list[dict[str, Any]]:  # type: ignore[override]
        self.statements.append(statement)
        for record_id, row in self.dataset.items():
            if record_id in statement:
                return [row]
        return []


@dataclass
class _FlaggerStub(MissingDataFlagger):
    calls: list[dict[str, Any]] = field(default_factory=list)

    def flag_missing(self, ticket_id: str, question: str, facts: dict[str, Any]) -> None:  # type: ignore[override]
        self.calls.append({"ticket_id": ticket_id, "question": question, "facts": facts})


@dataclass
class _ScraperStub:
    outcome: ScrapeOutcome
    calls: list[dict[str, Any]] = field(default_factory=list)

    def execute_plan(self, ticket_id: str, question: str, missing_facts: dict[str, Any]) -> ScrapeOutcome:  # type: ignore[override]
        self.calls.append(
            {
                "ticket_id": ticket_id,
                "question": question,
                "missing_facts": missing_facts,
            }
        )
        return self.outcome


def test_runner_executes_scenarios() -> None:
    loader = _ScenarioLoaderStub(
        scenarios=[
            {
                "ticket_id": "T-1",
                "question": "What is the business name?",
                "record_id": "row-1",
            }
        ]
    )
    executor = _SQLExecutorStub(dataset={"row-1": {"BRIZO_ID": "row-1", "BUSINESS_NAME": "Cafe"}})
    flagger = _FlaggerStub()
    scraper = _ScraperStub(outcome=ScrapeOutcome(tasks=[], findings=[]))
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    results = runner.execute(profile="dev")

    assert results == [
        {
            "ticket_id": "T-1",
            "record_id": "row-1",
            "question": "What is the business name?",
            "status": "answered",
            "answers": {"BUSINESS_NAME": "Cafe"},
        }
    ]
    assert not flagger.calls
    assert not scraper.calls


def test_runner_flags_missing_records(tmp_path) -> None:
    loader = _ScenarioLoaderStub(
        scenarios=[
            {
                "ticket_id": "T-2",
                "question": "What is the business name?",
                "record_id": "missing",
            }
        ]
    )
    executor = _SQLExecutorStub(dataset={})
    flagger = _FlaggerStub()
    scraper = _ScraperStub(
        outcome=ScrapeOutcome(
            tasks=[
                SearchTask(
                    query="What is the business name?",
                    topic="general",
                    description="General context",
                )
            ],
            findings=[{"url": "https://example.com"}],
        )
    )
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    results = runner.execute(profile="dev")

    result = results[0]
    assert result["status"] == "record_not_found"
    assert flagger.calls and flagger.calls[0]["facts"]["reason"] == "record_not_found"
    assert scraper.calls and scraper.calls[0]["missing_facts"]["status"] == "record_not_found"
    assert result["scraper_tasks"][0]["topic"] == "general"
    assert result["scraper_findings"] == 1


def test_yaml_scenario_loader_reads_profiles(tmp_path) -> None:
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    (scenarios_dir / "dev.yaml").write_text(
        "- ticket_id: T-1\n  question: What is the business name?\n  record_id: row-1\n",
        encoding="utf-8",
    )

    loader = YamlScenarioLoader(base_dir=scenarios_dir)

    scenarios = loader.load("dev")

    assert scenarios == [
        {
            "ticket_id": "T-1",
            "question": "What is the business name?",
            "record_id": "row-1",
        }
    ]
