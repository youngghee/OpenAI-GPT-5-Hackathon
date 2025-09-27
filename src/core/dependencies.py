"""Factory helpers for constructing agent dependencies from settings."""

from __future__ import annotations

import os
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
from src.core.observability import (
    JSONLQueryLogger,
    JSONLScraperLogger,
    QueryObservationSink,
    ScraperObservationSink,
)
from src.core.missing_data import JSONLMissingDataFlagger
from src.core.schema import JSONLSchemaEscalator
from src.integrations.csv_sql_executor import CsvSQLExecutor
from src.integrations.in_memory_sql_executor import InMemorySQLExecutor
from src.integrations.openai_models import GPTResponseClient, OpenAIClientFactory
from src.integrations.openai_agent_sdk import OpenAIAgentAdapter
from src.integrations.openai_search import OpenAIWebSearchClient


@dataclass(slots=True)
class RunnerDependencies:
    """Collection of optional dependencies used by the runner."""

    sql_executor: SQLExecutor | None = None
    missing_data_flagger: MissingDataFlagger | None = None
    scraper_agent: ScraperAgent | None = None
    update_agent: UpdateAgent | None = None
    schema_agent: SchemaAgent | None = None
    query_logger: QueryObservationSink | None = None
    scraper_logger: ScraperObservationSink | None = None
    gpt_client: OpenAIAgentAdapter | None = None
    candidate_url_fields: list[str] | None = None


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
    search_client: SearchClient = _build_search_client(settings)
    scraper_logs_dir = _resolve_scraper_logs_dir(settings)
    scraper_logger = JSONLScraperLogger(base_dir=scraper_logs_dir)
    scraper_agent = ScraperAgent(
        search_client=search_client,
        evidence_sink=evidence_sink,
        logger=scraper_logger,
        llm_client=None,  # placeholder; updated after response client construction
    )

    schema_dir = _resolve_schema_dir(settings)
    schema_escalator = JSONLSchemaEscalator(base_dir=schema_dir)
    migrations_dir = _resolve_migrations_dir(settings)
    migration_writer = FileMigrationWriter(base_dir=migrations_dir)
    crm_client = InMemoryCRMClient()
    update_agent = UpdateAgent(
        crm_client=crm_client,
        schema_escalator=schema_escalator,
        allowed_fields=None,
        llm_client=None,
    )
    schema_agent = SchemaAgent(migration_writer=migration_writer, llm_client=None)

    query_logs_dir = _resolve_query_logs_dir(settings)
    query_logger = JSONLQueryLogger(base_dir=query_logs_dir)
    response_client = _build_response_client(settings)
    agent_client = _build_agent_client(settings, response_client)
    candidate_url_fields = _detect_candidate_url_fields(executor)

    if agent_client is not None:
        scraper_agent.llm_client = agent_client
        update_agent.llm_client = agent_client
        schema_agent.llm_client = agent_client

    return RunnerDependencies(
        sql_executor=executor,
        missing_data_flagger=flagger,
        scraper_agent=scraper_agent,
        update_agent=update_agent,
        schema_agent=schema_agent,
        query_logger=query_logger,
        scraper_logger=scraper_logger,
        gpt_client=agent_client,
        candidate_url_fields=candidate_url_fields,
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


def _build_search_client(settings: Settings) -> SearchClient:
    search_settings = settings.search
    if search_settings and search_settings.provider == "openai":
        api_key = os.getenv(search_settings.api_key_env, "")
        if api_key:
            return OpenAIWebSearchClient(
                model=search_settings.model_id,
                api_key=api_key,
                max_results=search_settings.max_results,
            )
    return NullSearchClient()


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


def _resolve_query_logs_dir(settings: Settings) -> Path:
    base = (
        settings.paths.query_logs_dir
        if settings.paths and settings.paths.query_logs_dir
        else "logs/query"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_scraper_logs_dir(settings: Settings) -> Path:
    base = (
        settings.paths.scraper_logs_dir
        if settings.paths and settings.paths.scraper_logs_dir
        else "logs/scraper"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_response_client(settings: Settings) -> GPTResponseClient | None:
    if not settings.response_model_id:
        return None
    try:
        factory = OpenAIClientFactory()
        client = GPTResponseClient(model=settings.response_model_id, client_factory=factory)
        # trigger lazy init to validate configuration early
        client.client
    except Exception:
        return None
    return client


def _build_agent_client(
    settings: Settings, fallback: GPTResponseClient | None
) -> OpenAIAgentAdapter | None:
    if fallback is None:
        return None
    model_id = settings.response_model_id or fallback.model
    try:
        return OpenAIAgentAdapter(model=model_id, fallback=fallback)
    except Exception:
        return None


URL_FIELD_HINTS = {
    "LINK",
    "WEBSITE",
    "BRIZO_WEBSITE",
    "PARENT_URL",
    "CHAIN_URL",
    "GOOGLEMAPS_LINK",
    "YELP_LINK",
    "TRIPADVISOR_LINK",
    "INSTAGRAM_LINK",
    "FACEBOOK_LINK",
    "DOORDASH_LINK",
    "UBEREATS_LINK",
    "GRUBHUB_LINK",
    "EZCATER_LINK",
    "OPENTABLE_LINK",
    "SLICE_LINK",
}

URL_FIELD_EXACT_IGNORE = {"INTERNAL_URLS", "EXTERNAL_URLS"}
URL_FIELD_PARTIAL_IGNORE = {"LOGO", "IMAGE", "PHOTO"}


def _detect_candidate_url_fields(executor: SQLExecutor) -> list[str]:
    if isinstance(executor, CsvSQLExecutor):
        columns = executor.columns
    else:
        return []

    selected: list[str] = []
    for column in columns:
        upper = column.upper()
        if upper in URL_FIELD_EXACT_IGNORE:
            continue
        if any(part in upper for part in URL_FIELD_PARTIAL_IGNORE):
            continue
        if upper in URL_FIELD_HINTS:
            selected.append(column)
    return selected
