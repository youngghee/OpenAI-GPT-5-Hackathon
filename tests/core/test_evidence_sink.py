"""Tests for evidence sink implementations."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.evidence import JSONLEvidenceSink


def test_jsonl_evidence_sink_appends_lines(tmp_path: Path) -> None:
    sink = JSONLEvidenceSink(base_dir=tmp_path)

    sink.bulk_append(
        ticket_id="T-1",
        payloads=[
            {"query": "q1", "result": {"url": "https://example.com"}},
            {"query": "q2", "result": {"url": "https://example.org"}},
        ],
    )

    files = sorted(tmp_path.glob("*-T-1.jsonl"))
    assert len(files) == 1
    output_file = files[0]
    lines = [json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines()]
    expected_lines = 2
    assert len(lines) == expected_lines
    assert lines[0]["result"]["url"] == "https://example.com"
    assert all("timestamp" in entry for entry in lines)
