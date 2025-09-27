"""Factory helpers for constructing agent dependencies from settings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.agents.query_agent import MissingDataFlagger, SQLExecutor
from src.core.config import Settings
from src.core.missing_data import JSONLMissingDataFlagger
from src.integrations.csv_sql_executor import CsvSQLExecutor
from src.integrations.in_memory_sql_executor import InMemorySQLExecutor


@dataclass(slots=True)
class RunnerDependencies:
    """Collection of optional dependencies used by the runner."""

    sql_executor: SQLExecutor | None = None
    missing_data_flagger: MissingDataFlagger | None = None


def build_dependencies(settings: Settings) -> RunnerDependencies:
    """Create dependency instances based on *settings*."""

    if settings.csv_source is not None:
        path = settings.csv_source.resolve_path()
        executor = CsvSQLExecutor(csv_path=path, table_name=settings.csv_source.table_name)
    else:
        executor = InMemorySQLExecutor()

    scrapes_dir = _resolve_scrapes_dir(settings)
    flagger = JSONLMissingDataFlagger(base_dir=scrapes_dir)

    return RunnerDependencies(sql_executor=executor, missing_data_flagger=flagger)


def _resolve_scrapes_dir(settings: Settings) -> Path:
    base = (
        settings.paths.scrapes_dir
        if settings.paths and settings.paths.scrapes_dir
        else "assets/scrapes"
    )
    path = Path(base).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path
