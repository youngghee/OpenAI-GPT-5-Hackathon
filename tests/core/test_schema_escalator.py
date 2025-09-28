"""Tests for schema escalation persistence."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.schema import JSONLSchemaEscalator


def test_jsonl_schema_escalator_appends(tmp_path: Path) -> None:
    escalator = JSONLSchemaEscalator(base_dir=tmp_path)

    escalator.escalate(
        ticket_id="T-1",
        rationale={"unknown_fields": {"NEW_FIELD": "value"}},
    )

    files = sorted(tmp_path.glob("*-T-1.jsonl"))
    assert len(files) == 1
    output = files[0]
    payload = json.loads(output.read_text(encoding="utf-8").strip())
    assert payload["rationale"]["unknown_fields"]["NEW_FIELD"] == "value"
    assert "timestamp" in payload
