"""Tests for the CSV dataset inspector."""

from __future__ import annotations

from pathlib import Path

from src.integrations.csv_dataset import CsvDatasetInspector


def _create_sample_csv(tmp_path: Path) -> Path:
    content = """ID,NAME,CITY
1,Acme,New York
2,Bravo,Boston
3,Charlie,Chicago
"""
    csv_path = tmp_path / "dataset.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def test_inspector_describe(tmp_path: Path) -> None:
    csv_path = _create_sample_csv(tmp_path)
    inspector = CsvDatasetInspector(path=csv_path, max_preview_rows=2)

    summary = inspector.describe()

    expected_row_count = 3
    expected_column_count = 3
    expected_preview_rows = 2

    assert summary["row_count"] == expected_row_count
    assert summary["column_count"] == expected_column_count
    assert summary["columns"] == ["ID", "NAME", "CITY"]
    assert len(summary["preview_rows"]) == expected_preview_rows
    assert summary["preview_rows"][0]["NAME"] == "Acme"


def test_inspector_raises_without_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "broken.csv"
    csv_path.write_text("1,2,3", encoding="utf-8")
    inspector = CsvDatasetInspector(path=csv_path)

    try:
        inspector.describe()
    except ValueError as exc:
        assert "header" in str(exc)
    else:
        raise AssertionError("Expected ValueError due to missing header row")
