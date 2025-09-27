"""Factory helpers for constructing agent dependencies from settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.agents.query_agent import MissingDataFlagger, SQLExecutor
from src.agents.schema_agent import SchemaAgent
from src.agents.scraper_agent import ScraperAgent, SearchClient
from src.agents.update_agent import UpdateAgent
from src.core.config import Settings
from src.core.evidence import JSONLEvidenceSink
from src.core.migrations import FileMigrationWriter
from src.core.missing_data import JSONLMissingDataFlagger
from src.core.schema import JSONLSchemaEscalator
from src.integrations.csv_sql_executor import CsvSQLExecutor
from src.integrations.in_memory_sql_executor import InMemorySQLExecutor


@dataclass(slots=True)
class RunnerDependencies:
    """Collection of optional dependencies used by the runner."""

    sql_executor: SQLExecutor | None = None
    missing_data_flagger: MissingDataFlagger | None = None
    scraper_agent: ScraperAgent | None = None
    update_agent: UpdateAgent | None = None
    schema_agent: SchemaAgent | None = None


def build_dependencies(settings: Settings) -> RunnerDependencies:
    """Create dependency instances based on *settings*."""

    if settings.csv_source is not None:
        path = settings.csv_source.resolve_path()
        executor = CsvSQLExecutor(csv_path=path, table_name=settings.csv_source.table_name)
    else:
        executor = InMemorySQLExecutor()

    scrapes_dir = _resolve_scrapes_dir(settings)
    flagger = JSONLMissingDataFlagger(base_dir=scrapes_dir)
    evidence_sink = JSONLEvidenceSink(base_dir=scrapes_dir)
    search_client: SearchClient = NullSearchClient()
    scraper_agent = ScraperAgent(search_client=search_client, evidence_sink=evidence_sink)

    schema_dir = _resolve_schema_dir(settings)
    schema_escalator = JSONLSchemaEscalator(base_dir=schema_dir)
    migrations_dir = _resolve_migrations_dir(settings)
    migration_writer = FileMigrationWriter(base_dir=migrations_dir)
    crm_client = InMemoryCRMClient()
    update_agent = UpdateAgent(
        crm_client=crm_client,
        schema_escalator=schema_escalator,
        allowed_fields=None,
    )
    schema_agent = SchemaAgent(migration_writer=migration_writer)

    return RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper_agent,
        update_agent=update_agent,
        schema_agent=schema_agent,
    )


def _resolve_scrapes_dir(settings: Settings) -> Path:
    base = (
        settings.paths.scrapes_dir
        if settings.paths and settings.paths.scrapes_dir
        else "assets/scrapes"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


class NullSearchClient(SearchClient):
    """Fallback search client that returns no results."""

    def search(self, query: str, *, limit: int | None = None):  # type: ignore[override]
        return []


def _resolve_schema_dir(settings: Settings) -> Path:
    base = (
        settings.paths.schema_escalations_dir
        if settings.paths and settings.paths.schema_escalations_dir
        else "schema/escalations"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


class InMemoryCRMClient:
    """Simple in-memory CRM client used for local simulations."""

    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.history: list[dict[str, Any]] = []

    def update_record(self, record_id: str, payload: dict[str, Any]) -> None:
        existing = self.records.setdefault(record_id, {})
        existing.update(payload)
        self.history.append({"record_id": record_id, "payload": payload})


def _resolve_migrations_dir(settings: Settings) -> Path:
    base = (
        settings.paths.migrations_dir
        if settings.paths and settings.paths.migrations_dir
        else "schema/migrations"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path
