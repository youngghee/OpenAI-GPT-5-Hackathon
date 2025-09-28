"""Tests for the chat-based CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from src.agents.query_agent import MissingDataFlagger, SQLExecutor
from src.agents.scraper_agent import ScrapeOutcome
from src.core.dependencies import RunnerDependencies
from src.core.chat import ChatCLI


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
        self,
        *,
        ticket_id: str,
        record_id: str,
        facts: list[dict[str, Any]] | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(facts, dict):
            fact_list = [facts]
        else:
            fact_list = list(facts)
        summary = {
            "ticket_id": ticket_id,
            "record_id": record_id,
            "facts": fact_list,
        }
        self.summaries.append(summary)
        return {
            "status": "updated" if fact_list else "skipped",
            "applied_facts": fact_list,
        }


@dataclass
class _SchemaAgentStub:
    calls: list[dict[str, Any]] = field(default_factory=list)

    def propose_change(self, *, ticket_id: str, evidence_summary: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"ticket_id": ticket_id, "evidence_summary": evidence_summary})
        return {
            "ticket_id": ticket_id,
            "columns": [],
        }


def _make_dependencies(dataset: dict[str, dict[str, Any]]) -> RunnerDependencies:
    executor = _SQLExecutorStub(dataset=dataset)
    flagger = _FlaggerStub()
    scraper = _ScraperStub(outcome=ScrapeOutcome(tasks=[], findings=[]))
    updater = _UpdateAgentStub()
    schema_agent = _SchemaAgentStub()
    return RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper,
        update_agent=updater,
        schema_agent=schema_agent,
        query_logger=None,
        scraper_logger=None,
    )


def _input_factory(responses: list[str]) -> tuple[Callable[[str], str], list[str]]:
    iterator: Iterator[str] = iter(responses)
    prompts: list[str] = []

    def _input(prompt: str) -> str:
        prompts.append(prompt)
        try:
            return next(iterator)
        except StopIteration:
            raise EOFError

    return _input, prompts


def test_chat_cli_answers_question() -> None:
    dataset = {
        "row-1": {
            "BRIZO_ID": "row-1",
            "BUSINESS_NAME": "Cafe Example",
        }
    }
    dependencies = _make_dependencies(dataset)
    input_stub, _ = _input_factory(["What is the business name?", "/exit"])
    outputs: list[str] = []

    cli = ChatCLI(
        dependencies=dependencies,
        input_func=input_stub,
        output_func=outputs.append,
        session_id_factory=lambda: "SESSION",
    )
    cli.start(record_id="row-1")

    assert any("status: answered" in line for line in outputs)
    assert any("business_name" in line or "Cafe Example" in line for line in outputs)
    assert any(line.startswith("  ↳ Received question") for line in outputs)


def test_chat_cli_supports_record_switch() -> None:
    dataset = {
        "row-1": {"BRIZO_ID": "row-1", "BUSINESS_NAME": "Cafe One"},
        "row-2": {"BRIZO_ID": "row-2", "BUSINESS_NAME": "Cafe Two"},
    }
    dependencies = _make_dependencies(dataset)
    input_stub, prompts = _input_factory([
        "/record row-2",
        "What is the business name?",
        "/exit",
    ])
    outputs: list[str] = []

    cli = ChatCLI(
        dependencies=dependencies,
        input_func=input_stub,
        output_func=outputs.append,
        session_id_factory=lambda: "SESSION",
    )
    cli.start(record_id="row-1")

    assert "Active record set to row-2." in outputs
    assert any("Cafe Two" in line for line in outputs)
    assert any(line.startswith("  ↳ Received question") for line in outputs)
    # Ensure prompts reflect the active record id
    assert prompts and prompts[0].startswith("[row-1]")
