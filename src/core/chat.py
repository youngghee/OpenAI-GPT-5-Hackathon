"""Interactive chat interface for running query scenarios."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from src.core.config import load_settings
from src.core.dependencies import RunnerDependencies, build_dependencies
from src.core.runner import run_scenario

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

    def start(self, record_id: str | None = None) -> None:
        """Launch an interactive chat session."""

        session_id = self.session_id_factory()
        record = record_id or self._ask_non_empty("Enter record id: ")

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
