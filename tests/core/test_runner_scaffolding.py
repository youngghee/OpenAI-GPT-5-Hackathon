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


@dataclass
class _UpdateAgentStub:
    summaries: list[dict[str, Any]] = field(default_factory=list)

    def apply_enrichment(
        self, *, ticket_id: str, record_id: str, enriched_fields: dict[str, Any]
    ) -> dict[str, Any]:
        summary = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "fields": enriched_fields,
        }
        self.summaries.append(summary)
        response: dict[str, Any] = {"status": "simulated", "fields": enriched_fields}
        unknown = {key: value for key, value in enriched_fields.items() if key.startswith("NEW")}
        if unknown:
            response["escalated"] = {"unknown_fields": unknown}
        return response


@dataclass
class _SchemaAgentStub:
    calls: list[dict[str, Any]] = field(default_factory=list)

    def propose_change(self, *, ticket_id: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"ticket_id": ticket_id, "evidence_summary": evidence_summary})
        return {
            "ticket_id": ticket_id,
            "columns": ["NEW_FIELD"],
            "migration_path": "schema/migrations/test.sql",
        }


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
    updater = _UpdateAgentStub()
    schema_agent = _SchemaAgentStub()
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
        update_agent=updater,
        schema_agent=schema_agent,
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    results = runner.execute(profile="dev")

    assert len(results) == 1
    result = results[0]
    assert result["status"] == "answered"
    assert result["answers"] == {"BUSINESS_NAME": "Cafe"}
    assert result["update"]["status"] == "simulated"
    assert not flagger.calls
    assert not scraper.calls
    assert updater.summaries and updater.summaries[0]["fields"] == {"BUSINESS_NAME": "Cafe"}
    assert not schema_agent.calls


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
    updater = _UpdateAgentStub()
    schema_agent = _SchemaAgentStub()
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
        update_agent=updater,
        schema_agent=schema_agent,
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    results = runner.execute(profile="dev")

    result = results[0]
    assert result["status"] == "record_not_found"
    assert flagger.calls and flagger.calls[0]["facts"]["reason"] == "record_not_found"
    assert scraper.calls and scraper.calls[0]["missing_facts"]["status"] == "record_not_found"
    assert result["scraper_tasks"][0]["topic"] == "general"
    assert result["scraper_findings"] == 1
    assert not updater.summaries
    assert not schema_agent.calls


def test_runner_invokes_schema_agent_on_escalation() -> None:
    loader = _ScenarioLoaderStub(
        scenarios=[
            {
                "ticket_id": "T-3",
                "question": "What is the business name?",
                "record_id": "row-2",
                "enriched_fields": {"NEW_METRIC": 12},
            }
        ]
    )
    executor = _SQLExecutorStub(dataset={"row-2": {"BRIZO_ID": "row-2", "BUSINESS_NAME": "Cafe"}})
    flagger = _FlaggerStub()
    scraper = _ScraperStub(outcome=ScrapeOutcome(tasks=[], findings=[]))
    updater = _UpdateAgentStub()
    schema_agent = _SchemaAgentStub()
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
        update_agent=updater,
        schema_agent=schema_agent,
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    result = runner.execute(profile="dev")[0]

    assert schema_agent.calls
    assert schema_agent.calls[0]["ticket_id"] == "T-3"
    assert "schema_proposal" in result


def test_scraper_receives_candidate_urls() -> None:
    loader = _ScenarioLoaderStub(
        scenarios=[
            {
                "ticket_id": "T-4",
                "question": "How many employees?",
                "record_id": "row-url",
            }
        ]
    )
    executor = _SQLExecutorStub(
        dataset={"row-url": {"BRIZO_ID": "row-url", "LINK": "example.com"}}
    )
    flagger = _FlaggerStub()
    scraper = _ScraperStub(outcome=ScrapeOutcome(tasks=[], findings=[]))
    updater = _UpdateAgentStub()
    schema_agent = _SchemaAgentStub()
    deps = RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
        update_agent=updater,
        schema_agent=schema_agent,
        candidate_url_fields=["LINK"],
    )

    runner = Runner(scenario_loader=loader, dependencies=deps)

    results = runner.execute(profile="dev")

    assert results and results[0]["status"] == "unknown_question"
    assert scraper.calls, "Scraper should be invoked for unanswered questions"
    missing_facts = scraper.calls[0]["missing_facts"]
    assert missing_facts.get("candidate_urls") == ["https://example.com"]


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
