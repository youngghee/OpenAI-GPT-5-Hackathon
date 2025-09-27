"""Tests for dependency construction."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.core.config import (
    AgentSettings,
    CSVSourceSettings,
    PathsSettings,
    ScraperSettings,
    SearchSettings,
    Settings,
)
from src.core.dependencies import NullSearchClient, RunnerDependencies, build_dependencies


@pytest.fixture()
def base_settings() -> Settings:
    return Settings(
        model_id="gpt-5-enterprise",
        codex_id="gpt-5-codex-pro",
        response_model_id="",
        scraper=ScraperSettings(rate_limit_per_min=30, default_timeout_s=15),
        csv_source=None,
        paths=PathsSettings(scrapes_dir=None, schema_escalations_dir=None, migrations_dir=None),
        search=None,
        agents={"query": AgentSettings(token_budget=1, safety_notes=[])},
    )


def test_build_dependencies_uses_in_memory_when_no_csv(base_settings: Settings) -> None:
    deps = build_dependencies(base_settings)
    assert isinstance(deps, RunnerDependencies)
    assert deps.sql_executor is not None
    assert deps.missing_data_flagger is not None
    assert isinstance(deps.scraper_agent.search_client, NullSearchClient)
    assert deps.update_agent is not None
    assert deps.schema_agent is not None
    assert deps.query_logger is not None
    assert deps.scraper_logger is not None
    assert deps.gpt_client is None
    assert deps.candidate_url_fields == []


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
    assert deps.query_logger is not None
    assert deps.scraper_logger is not None
    assert deps.gpt_client is None
    assert deps.candidate_url_fields == []


def test_build_dependencies_uses_openai_search(
    monkeypatch: pytest.MonkeyPatch, base_settings: Settings
) -> None:
    class _SearchStub:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

        def search(
            self, query: str, *, limit: int | None = None
        ) -> list[dict[str, Any]]:  # pragma: no cover - not used
            return []

    created_clients: list[_SearchStub] = []

    def _factory(*args: Any, **kwargs: Any) -> _SearchStub:
        client = _SearchStub(*args, **kwargs)
        created_clients.append(client)
        return client

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        "src.core.dependencies.OpenAIWebSearchClient",
        lambda *args, **kwargs: _factory(*args, **kwargs),
    )

    base_settings.search = SearchSettings(
        provider="openai",
        model_id="gpt-4.1-mini",
        max_results=5,
        api_key_env="OPENAI_API_KEY",
    )

    deps = build_dependencies(base_settings)

    expected_max_results = 5
    assert isinstance(deps.scraper_agent.search_client, _SearchStub)
    assert created_clients and created_clients[0].kwargs["max_results"] == expected_max_results
    assert deps.candidate_url_fields == []


def test_candidate_url_fields_detected(
    tmp_path: Path, base_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv_file = tmp_path / "records.csv"
    csv_file.write_text(
        (
            "BRIZO_ID,LINK,YELP_LINK,BRIZO_WEBSITE,NOTES\n"
            "row-1,https://example.com,https://yelp.com/biz/example,pigglywiggly.com,info\n"
        ),
        encoding="utf-8",
    )
    base_settings.csv_source = CSVSourceSettings(path_env="CSV_DATA_PATH", table_name="records")
    monkeypatch.setenv("CSV_DATA_PATH", str(csv_file))

    deps = build_dependencies(base_settings)

    assert set(deps.candidate_url_fields or []) == {"LINK", "YELP_LINK", "BRIZO_WEBSITE"}
