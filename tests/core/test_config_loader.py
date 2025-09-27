"""Tests for loading application settings from YAML."""

# ruff: noqa: PLR2004

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.config import load_settings


def test_load_settings_parses_agents(tmp_path: Path) -> None:
    config_path = tmp_path / "dev.yaml"
    config_path.write_text(
        """
model_id: foo
codex_id: bar
scraper:
  rate_limit_per_min: 10
  default_timeout_s: 5
agents:
  query:
    token_budget: 123
    safety_notes: ["note"]
        """,
        encoding="utf-8",
    )

    settings = load_settings(config_path)

    assert settings.model_id == "foo"
    assert settings.scraper.rate_limit_per_min == 10
    assert settings.agents["query"].token_budget == 123


def test_load_settings_handles_csv_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_file = tmp_path / "records.csv"
    csv_file.write_text("id\n1\n", encoding="utf-8")
    config_path = tmp_path / "dev.yaml"
    config_path.write_text(
        """
model_id: foo
codex_id: bar
scraper:
  rate_limit_per_min: 10
  default_timeout_s: 5
data_sources:
  csv:
    path_env: CSV_DATA_PATH
    table_name: dataset
agents: {}
        """,
        encoding="utf-8",
    )
    monkeypatch.setenv("CSV_DATA_PATH", str(csv_file))

    settings = load_settings(config_path)
    assert settings.csv_source is not None
    assert settings.csv_source.resolve_path() == csv_file
