"""Tests for JSONL observability sinks."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.observability import JSONLQueryLogger, JSONLScraperLogger


def _load_events(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def test_jsonl_query_logger_appends_events(tmp_path: Path) -> None:
    logger = JSONLQueryLogger(base_dir=tmp_path)

    logger.log_event("T-1", "question_received", {"record_id": "abc"})

    files = sorted(tmp_path.glob("*.jsonl"))
    assert len(files) == 1
    target = files[0]
    assert target.name.endswith("-T-1.jsonl")
    events = _load_events(target)
    assert events[0]["event"] == "question_received"
    assert events[0]["record_id"] == "abc"
    assert "timestamp" in events[0]


def test_jsonl_scraper_logger_appends_events(tmp_path: Path) -> None:
    logger = JSONLScraperLogger(base_dir=tmp_path)

    logger.log_event("T-2", "scrape_task_started", {"topic": "BUSINESS_NAME"})
    logger.log_event("T-2", "scrape_task_completed", {"result_count": 3})

    files = sorted(tmp_path.glob("*-T-2.jsonl"))
    assert len(files) == 1
    target = files[0]
    events = _load_events(target)
    assert len(events) == 2
    assert events[0]["event"] == "scrape_task_started"
    assert events[1]["event"] == "scrape_task_completed"
    assert events[1]["result_count"] == 3
