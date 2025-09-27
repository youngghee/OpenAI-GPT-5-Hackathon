"""Factory helpers for constructing agent dependencies from settings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.core.config import Settings
from src.integrations.csv_sql_executor import CsvSQLExecutor
from src.integrations.in_memory_sql_executor import InMemorySQLExecutor


@dataclass(slots=True)
class RunnerDependencies:
    """Collection of optional dependencies used by the runner."""

    sql_executor: Optional[object] = None


def build_dependencies(settings: Settings) -> RunnerDependencies:
    """Create dependency instances based on *settings*."""

    if settings.csv_source is not None:
        path = settings.csv_source.resolve_path()
        executor = CsvSQLExecutor(csv_path=path, table_name=settings.csv_source.table_name)
        return RunnerDependencies(sql_executor=executor)

    # Fallback to in-memory executor when no CSV source is configured.
    return RunnerDependencies(sql_executor=InMemorySQLExecutor())
