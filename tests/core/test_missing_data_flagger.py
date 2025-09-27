"""Tests for the JSONL missing data flagger."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.missing_data import JSONLMissingDataFlagger


def test_jsonl_flagger_writes_payload(tmp_path: Path) -> None:
    flagger = JSONLMissingDataFlagger(base_dir=tmp_path)

    flagger.flag_missing(
        ticket_id="T-1",
        question="What is the business name?",
        facts={"reason": "missing_values"},
    )

    output_file = tmp_path / "T-1.jsonl"
    assert output_file.exists()
    content = output_file.read_text(encoding="utf-8").strip()
    payload = json.loads(content)
    assert payload["facts"]["reason"] == "missing_values"
