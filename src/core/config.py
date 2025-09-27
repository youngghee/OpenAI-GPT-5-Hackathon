"""Utilities for loading application settings from YAML configuration files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class ScraperSettings:
    rate_limit_per_min: int
    default_timeout_s: int


@dataclass(slots=True)
class CSVSourceSettings:
    path_env: str
    table_name: str

    def resolve_path(self) -> Path:
        value = os.getenv(self.path_env)
        if not value:
            raise OSError(f"Environment variable '{self.path_env}' is required for CSV data source")
        path = Path(value).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"CSV data source not found at '{path}'")
        return path


@dataclass(slots=True)
class PathsSettings:
    scrapes_dir: str | None = None
    schema_escalations_dir: str | None = None
    migrations_dir: str | None = None
    query_logs_dir: str | None = None
    scraper_logs_dir: str | None = None


@dataclass(slots=True)
class AgentSettings:
    token_budget: int
    safety_notes: list[str]


@dataclass(slots=True)
class SearchSettings:
    provider: str
    model_id: str
    max_results: int
    api_key_env: str


@dataclass(slots=True)
class Settings:
    model_id: str
    codex_id: str
    scraper: ScraperSettings
    csv_source: CSVSourceSettings | None
    paths: PathsSettings | None
    search: SearchSettings | None
    agents: dict[str, AgentSettings]


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_settings(path: str | Path) -> Settings:
    """Read configuration from *path* and return structured settings."""

    config_path = Path(path)
    raw = _load_yaml(config_path)

    scraper_raw = raw.get("scraper", {})
    scraper = ScraperSettings(
        rate_limit_per_min=int(scraper_raw.get("rate_limit_per_min", 0)),
        default_timeout_s=int(scraper_raw.get("default_timeout_s", 0)),
    )

    data_sources = raw.get("data_sources", {})
    csv_source_raw = data_sources.get("csv")
    csv_source = None
    if csv_source_raw:
        csv_source = CSVSourceSettings(
            path_env=str(csv_source_raw.get("path_env")),
            table_name=str(csv_source_raw.get("table_name", "dataset")),
        )

    paths_raw: dict[str, Any] | None = raw.get("paths")
    paths = None
    if paths_raw:
        scrapes_dir = paths_raw.get("scrapes_dir")
        schema_dir = paths_raw.get("schema_escalations_dir")
        migrations_dir = paths_raw.get("migrations_dir")
        query_logs_dir = paths_raw.get("query_logs_dir")
        scraper_logs_dir = paths_raw.get("scraper_logs_dir")
        paths = PathsSettings(
            scrapes_dir=str(scrapes_dir) if scrapes_dir else None,
            schema_escalations_dir=str(schema_dir) if schema_dir else None,
            migrations_dir=str(migrations_dir) if migrations_dir else None,
            query_logs_dir=str(query_logs_dir) if query_logs_dir else None,
            scraper_logs_dir=str(scraper_logs_dir) if scraper_logs_dir else None,
        )

    search_raw = raw.get("search")
    search = None
    if search_raw:
        search = SearchSettings(
            provider=str(search_raw.get("provider", "")).lower(),
            model_id=str(search_raw.get("model_id", "")),
            max_results=int(search_raw.get("max_results", 5)),
            api_key_env=str(search_raw.get("api_key_env", "OPENAI_API_KEY")),
        )

    agents_raw = raw.get("agents", {})
    agents: dict[str, AgentSettings] = {}
    for name, values in agents_raw.items():
        agents[name] = AgentSettings(
            token_budget=int(values.get("token_budget", 0)),
            safety_notes=list(values.get("safety_notes", [])),
        )

    return Settings(
        model_id=str(raw.get("model_id", "")),
        codex_id=str(raw.get("codex_id", "")),
        scraper=scraper,
        csv_source=csv_source,
        paths=paths,
        search=search,
        agents=agents,
    )
