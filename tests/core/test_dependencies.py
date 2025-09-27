"""Tests for dependency construction."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.config import (
    AgentSettings,
    CSVSourceSettings,
    PathsSettings,
    ScraperSettings,
    Settings,
)
from src.core.dependencies import RunnerDependencies, build_dependencies


@pytest.fixture()
def base_settings() -> Settings:
    return Settings(
        model_id="gpt-5-enterprise",
        codex_id="gpt-5-codex-pro",
        scraper=ScraperSettings(rate_limit_per_min=30, default_timeout_s=15),
        csv_source=None,
        paths=PathsSettings(scrapes_dir=None, schema_escalations_dir=None, migrations_dir=None),
        agents={"query": AgentSettings(token_budget=1, safety_notes=[])},
    )


def test_build_dependencies_uses_in_memory_when_no_csv(base_settings: Settings) -> None:
    deps = build_dependencies(base_settings)
    assert isinstance(deps, RunnerDependencies)
    assert deps.sql_executor is not None
    assert deps.missing_data_flagger is not None
    assert deps.scraper_agent is not None
    assert deps.update_agent is not None
    assert deps.schema_agent is not None


def test_build_dependencies_uses_csv_path(
    tmp_path: Path, base_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv_file = tmp_path / "records.csv"
    csv_file.write_text("id\n1\n", encoding="utf-8")
    base_settings.csv_source = CSVSourceSettings(path_env="CSV_DATA_PATH", table_name="records")
    monkeypatch.setenv("CSV_DATA_PATH", str(csv_file))

    deps = build_dependencies(base_settings)

    assert deps.sql_executor is not None
    assert deps.sql_executor.__class__.__name__ == "CsvSQLExecutor"
    assert deps.missing_data_flagger is not None
    assert deps.scraper_agent is not None
    assert deps.update_agent is not None
    assert deps.schema_agent is not None
