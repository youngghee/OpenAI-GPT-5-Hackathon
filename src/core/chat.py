"""Interactive chat interface for running query scenarios."""

from __future__ import annotations

import argparse
import json

from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from src.core.config import load_settings
from src.core.dependencies import RunnerDependencies, build_dependencies
from src.core.runner import run_scenario
from src.core.observability import QueryObservationSink, ScraperObservationSink
from src.agents.update_agent import UpdateAgent
from src.agents.schema_agent import SchemaAgent

_exit_commands = {"/exit", "exit", "quit", ":q"}


@dataclass
class ChatCLI:
    """Simple terminal chat experience built on top of the runner workflow."""

    dependencies: RunnerDependencies
    input_func: Callable[[str], str] = field(default=input)
    output_func: Callable[[str], None] = field(default=print)
    primary_key_column: str = "BRIZO_ID"
    table_name: str = "dataset"
    session_id_factory: Callable[[], str] = field(default=lambda: f"session-{uuid4().hex[:8]}")
    _observers_attached: bool = field(default=False, init=False)

    def start(self, record_id: str | None = None) -> None:
        """Launch an interactive chat session."""

        session_id = self.session_id_factory()
        record = record_id or self._ask_non_empty("Enter record id: ")

        self._attach_observers()

        self.output_func(
            "Type questions to query the dataset. Use '/record <id>' to switch context,"
            " and '/exit' to leave."
        )

        counter = 1
        while True:
            try:
                prompt = f"[{record}]> "
                raw = self.input_func(prompt)
            except EOFError:
                self.output_func("\nSession ended.")
                break

            question = raw.strip()
            if not question:
                continue
            if question.lower() in _exit_commands:
                self.output_func("Session ended.")
                break
            if question.startswith("/record"):
                record = self._handle_record_command(question, record)
                continue

            conversation_ticket = f"{session_id}-Q{counter:03d}"
            scenario = {
                "ticket_id": conversation_ticket,
                "question": question,
                "record_id": record,
                "primary_key_column": self.primary_key_column,
                "table_name": self.table_name,
            }

            try:
                result = run_scenario(self.dependencies, scenario)
            except Exception as exc:  # pragma: no cover - defensive
                self.output_func(f"Error: {exc}")
                continue

            self._render_response(conversation_ticket, result)
            counter += 1

    def _handle_record_command(self, command: str, current: str) -> str:
        parts = command.split(maxsplit=1)
        if len(parts) == 2 and parts[1].strip():
            new_record = parts[1].strip()
            self.output_func(f"Active record set to {new_record}.")
            return new_record
        self.output_func("Usage: /record <record_id>")
        return current

    def _render_response(self, conversation_ticket: str, result: dict[str, Any]) -> None:
        status = str(result.get("status", "unknown"))
        self.output_func(f"[{conversation_ticket}] status: {status}")

        answers = result.get("answers")
        if isinstance(answers, dict) and answers:
            self.output_func("Answers:")
            for key, value in answers.items():
                self.output_func(f"  - {key}: {value}")

        missing = result.get("missing_columns")
        if isinstance(missing, list) and missing:
            self.output_func("Missing columns:")
            for column in missing:
                self.output_func(f"  - {column}")

        scraper_tasks = result.get("scraper_tasks")
        if isinstance(scraper_tasks, list) and scraper_tasks:
            self.output_func("Scraper tasks:")
            for task in scraper_tasks:
                topic = task.get("topic") if isinstance(task, dict) else None
                query = task.get("query") if isinstance(task, dict) else None
                self.output_func(f"  - {topic}: {query}")

        findings = result.get("scraper_findings")
        if isinstance(findings, int) and findings:
            self.output_func(f"Scraper findings: {findings}")

        update_summary = result.get("update")
        if isinstance(update_summary, dict) and update_summary:
            status_text = update_summary.get("status")
            applied = update_summary.get("applied_fields")
            self.output_func("Update summary:")
            if status_text:
                self.output_func(f"  - status: {status_text}")
            if isinstance(applied, list) and applied:
                self.output_func("  - applied fields: " + ", ".join(applied))

            escalated = update_summary.get("escalated")
            if isinstance(escalated, dict) and escalated:
                self.output_func("  - escalated: yes")

        schema_proposal = result.get("schema_proposal")
        if isinstance(schema_proposal, dict) and schema_proposal.get("columns"):
            self.output_func("Schema proposal:")
            columns = schema_proposal.get("columns", [])
            for column in columns:
                if isinstance(column, dict):
                    name = column.get("name")
                    data_type = column.get("data_type")
                    self.output_func(f"  - {name}: {data_type}")

        self.output_func("")

    def _ask_non_empty(self, prompt: str) -> str:
        while True:
            value = self.input_func(prompt).strip()
            if value:
                return value
            self.output_func("Value cannot be empty.")

    def _attach_observers(self) -> None:
        if self._observers_attached:
            return

        downstream_query = getattr(self.dependencies, "query_logger", None)
        query_logger = ChatQueryLogger(downstream=downstream_query, emit=self.output_func)
        self.dependencies.query_logger = query_logger

        downstream_scraper = getattr(self.dependencies, "scraper_logger", None)
        scraper_logger = ChatScraperLogger(downstream=downstream_scraper, emit=self.output_func)
        self.dependencies.scraper_logger = scraper_logger

        if self.dependencies.scraper_agent is not None:
            self.dependencies.scraper_agent.logger = scraper_logger

        if self.dependencies.update_agent is not None:
            self.dependencies.update_agent = ChatUpdateAgent(
                inner=self.dependencies.update_agent,
                emit=self.output_func,
            )

        if self.dependencies.schema_agent is not None:
            self.dependencies.schema_agent = ChatSchemaAgent(
                inner=self.dependencies.schema_agent,
                emit=self.output_func,
            )

        self._observers_attached = True


@dataclass(slots=True)
class ChatQueryLogger(QueryObservationSink):
    downstream: QueryObservationSink | None
    emit: Callable[[str], None]

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        if self.downstream is not None:
            self.downstream.log_event(ticket_id, event, payload)
        message = describe_query_event(event, payload)
        if message:
            self.emit(f"  ↳ {message}")


@dataclass(slots=True)
class ChatScraperLogger(ScraperObservationSink):
    downstream: ScraperObservationSink | None
    emit: Callable[[str], None]

    def log_event(self, ticket_id: str, event: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        if self.downstream is not None:
            self.downstream.log_event(ticket_id, event, payload)
        message = describe_scraper_event(event, payload)
        if message:
            self.emit(f"  ↳ {message}")


@dataclass(slots=True)
class ChatUpdateAgent:
    inner: UpdateAgent
    emit: Callable[[str], None]

    def __getattr__(self, item: str) -> Any:
        return getattr(self.inner, item)

    def apply_enrichment(
        self,
        *,
        ticket_id: str,
        record_id: str,
        enriched_fields: dict[str, Any],
    ) -> dict[str, Any]:
        if enriched_fields:
            self.emit("  ↳ Passing new fields to the update agent.")
        else:
            self.emit("  ↳ Update agent check with no new fields provided.")
        result = self.inner.apply_enrichment(
            ticket_id=ticket_id, record_id=record_id, enriched_fields=enriched_fields
        )
        status = result.get("status")
        if status:
            self.emit(f"  ↳ Update agent finished with status '{status}'.")
        return result


@dataclass(slots=True)
class ChatSchemaAgent:
    inner: SchemaAgent
    emit: Callable[[str], None]

    def __getattr__(self, item: str) -> Any:
        return getattr(self.inner, item)

    def propose_change(
        self,
        *,
        ticket_id: str,
        evidence_summary: dict[str, Any],
    ) -> dict[str, Any]:
        self.emit("  ↳ Escalating to schema agent for review.")
        result = self.inner.propose_change(ticket_id=ticket_id, evidence_summary=evidence_summary)
        columns = result.get("columns") or []
        if columns:
            self.emit(
                "  ↳ Schema agent proposed columns: "
                + ", ".join(str(col.get("name")) for col in columns if isinstance(col, dict))
            )
        else:
            self.emit("  ↳ Schema agent found no structural changes needed.")
        return result


def describe_query_event(event: str, payload: dict[str, Any]) -> str:
    if event == "question_received":
        question = payload.get("question")
        return f"Received question: {question}" if question else "Received question."
    if event == "sql_executed":
        statement = payload.get("statement", "SQL executed")
        return f"Ran SQL to fetch the record." if statement else "Ran SQL query."
    if event == "record_fetch_result":
        found = payload.get("found")
        return "Record found in dataset." if found else "No record found for the requested id."
    if event == "columns_inferred":
        columns = payload.get("columns")
        if columns:
            return "Identified relevant columns: " + ", ".join(str(c) for c in columns)
        return "Could not map the question to existing columns."
    if event == "missing_data_flagged":
        reason = payload.get("facts", {}).get("reason", "missing data")
        return f"Flagged missing data ({reason}); asking the scraper agent to investigate."
    if event == "answer_ready":
        columns = payload.get("columns", [])
        if columns:
            return "Answer ready with columns: " + ", ".join(str(c) for c in columns)
        return "Answer ready."
    if event == "llm_answer":
        columns = payload.get("columns", [])
        if columns:
            return "LLM supplied values for: " + ", ".join(str(c) for c in columns)
        return "LLM produced an answer."
    if event == "question_resolved":
        status = payload.get("status")
        return f"Completed question with status '{status}'."
    if event == "llm_error":
        return "LLM request failed; falling back to deterministic logic."
    return f"Query event: {event}"


def describe_scraper_event(event: str, payload: dict[str, Any]) -> str:
    if event == "scrape_plan_created":
        count = payload.get("task_count", 0)
        return f"Prepared {count} follow-up search task(s)."
    if event == "llm_plan_created":
        count = payload.get("task_count", 0)
        return f"LLM generated {count} search suggestion(s)."
    if event == "scrape_task_started":
        topic = payload.get("topic", "general")
        return f"Searching for '{topic}'."
    if event == "scrape_task_completed":
        topic = payload.get("topic", "general")
        results = payload.get("result_count", 0)
        return f"Finished searching '{topic}' with {results} result(s)."
    if event == "scrape_findings_persisted":
        count = payload.get("count", 0)
        return f"Saved {count} finding(s) to the evidence log."
    if event == "scrape_no_findings":
        return "Search completed with no new findings."
    if event == "llm_error":
        return "LLM search planning failed; using fallback heuristics."
    return f"Scraper event: {event}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat-based interface for the query agent")
    parser.add_argument("--config", default="configs/dev.yaml", help="Path to the YAML config file")
    parser.add_argument("--record", help="Initial record id to inspect")
    parser.add_argument("--table", default="dataset", help="Table name to query")
    parser.add_argument(
        "--primary-key",
        default="BRIZO_ID",
        dest="primary_key",
        help="Primary key column used to fetch records",
    )
    args = parser.parse_args()

    settings = load_settings(args.config)
    dependencies = build_dependencies(settings)

    cli = ChatCLI(
        dependencies=dependencies,
        primary_key_column=args.primary_key,
        table_name=args.table,
    )
    cli.start(record_id=args.record)


if __name__ == "__main__":
    main()
