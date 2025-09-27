"""Tests for the FastAPI frontend."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from src.core.webapp import create_app


def _write_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
model_id: gpt-5-enterprise
codex_id: gpt-5-codex-pro
response_model_id: gpt-5
paths:
  scrapes_dir: {tmp_path / 'scrapes'}
  schema_escalations_dir: {tmp_path / 'schema'}
  migrations_dir: {tmp_path / 'migrations'}
  query_logs_dir: {tmp_path / 'logs' / 'query'}
  scraper_logs_dir: {tmp_path / 'logs' / 'scraper'}
search:
  provider: openai
  model_id: gpt-4.1-mini
  max_results: 5
  api_key_env: OPENAI_API_KEY
data_sources:
  csv:
    path_env: CSV_DATA_PATH
    table_name: dataset
agents:
  query:
    token_budget: 1000
    safety_notes: []
  scraper:
    token_budget: 1000
    safety_notes: []
  update:
    token_budget: 1000
    safety_notes: []
  schema:
    token_budget: 1000
    safety_notes: []
""",
        encoding="utf-8",
    )
    return config


@pytest.fixture()
def sample_dataset(tmp_path: Path) -> Path:
    csv_path = tmp_path / "dataset.csv"
    csv_path.write_text(
        """BRIZO_ID,BUSINESS_NAME,WEBSITE,LOCATION_CITY\nrow-1,Cafe Example,example.com,Seattle\n""",
        encoding="utf-8",
    )
    return csv_path


def test_start_session_returns_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, sample_dataset: Path) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("CSV_DATA_PATH", str(sample_dataset))

    app = create_app(config_path=str(config_path))
    with TestClient(app) as client:
        response = client.post("/api/session", json={"record_id": "row-1"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["record_context"]["BUSINESS_NAME"] == "Cafe Example"
        assert "https://example.com" in payload["candidate_urls"]
        assert payload["session_id"]


def test_ask_question_returns_answer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, sample_dataset: Path) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("CSV_DATA_PATH", str(sample_dataset))

    app = create_app(config_path=str(config_path))
    with TestClient(app) as client:
        start = client.post("/api/session", json={"record_id": "row-1"})
        assert start.status_code == 200
        session_id = start.json()["session_id"]

        answer = client.post(
            f"/api/session/{session_id}/ask",
            json={"question": "What is the business name?"},
        )
        assert answer.status_code == 202
        payload = answer.json()
        assert payload["status"] == "processing"
        ticket_id = payload["ticket_id"]

        result_payload = None
        for _ in range(5):
            response = client.get(f"/api/tickets/{ticket_id}")
            if response.status_code == 200:
                result_payload = response.json()
                break
            time.sleep(0.01)

        assert result_payload is not None, "ticket did not complete in time"
        assert result_payload["result"]["status"] == "answered"
        assert result_payload["result"]["answers"]["BUSINESS_NAME"] == "Cafe Example"
        timeline_messages = [entry["message"] for entry in result_payload["timeline"]]
        assert any("Received question" in message for message in timeline_messages)
