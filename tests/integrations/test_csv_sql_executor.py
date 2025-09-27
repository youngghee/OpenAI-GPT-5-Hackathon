"""Tests for the CSV-backed SQL executor."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.integrations.csv_sql_executor import CsvSQLExecutor


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    content = """ID,NAME,CITY
1,Acme,New York
2,Bravo,Boston
"""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def test_select_all_columns(sample_csv: Path) -> None:
    executor = CsvSQLExecutor(csv_path=sample_csv, table_name="records")

    rows = executor.run("SELECT * FROM records WHERE id = '1'")

    assert rows == [{"ID": "1", "NAME": "Acme", "CITY": "New York"}]


def test_select_specific_columns(sample_csv: Path) -> None:
    executor = CsvSQLExecutor(csv_path=sample_csv, table_name="records")

    rows = executor.run("SELECT name, city FROM RECORDS WHERE id = '2' LIMIT 1")

    assert rows == [{"NAME": "Bravo", "CITY": "Boston"}]


def test_unknown_column_raises(sample_csv: Path) -> None:
    executor = CsvSQLExecutor(csv_path=sample_csv, table_name="records")

    with pytest.raises(KeyError):
        executor.run("SELECT * FROM records WHERE missing = 'value'")


def test_invalid_statement_raises(sample_csv: Path) -> None:
    executor = CsvSQLExecutor(csv_path=sample_csv, table_name="records")

    with pytest.raises(NotImplementedError):
        executor.run("DELETE FROM records WHERE id = '1'")
