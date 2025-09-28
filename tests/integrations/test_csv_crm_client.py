"""Tests for the CSV-backed CRM client."""

from __future__ import annotations

from pathlib import Path

from src.integrations.csv_crm_client import CsvCRMClient
from src.integrations.csv_sql_executor import CsvSQLExecutor


def _write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_csv_crm_client_updates_row(tmp_path: Path) -> None:
    csv_path = tmp_path / "records.csv"
    _write_csv(
        csv_path,
        """BRIZO_ID,BUSINESS_NAME,LOCATION_CITY\n"
        "row-1,Cafe Example,Seattle\n"
        "row-2,Tea House,Portland\n""",
    )

    executor = CsvSQLExecutor(csv_path=csv_path, table_name="dataset")
    client = CsvCRMClient(executor=executor, primary_key="BRIZO_ID")

    client.update_record("row-1", {"BUSINESS_NAME": "Cafe Updated", "LOCATION_CITY": "Tacoma"})

    # Ensure in-memory rows are updated
    rows = executor.run("SELECT * FROM dataset WHERE BRIZO_ID = 'row-1'")
    assert rows[0]["BUSINESS_NAME"] == "Cafe Updated"
    assert rows[0]["LOCATION_CITY"] == "Tacoma"

    # Ensure the CSV file has been rewritten
    refreshed = csv_path.read_text(encoding="utf-8")
    assert "Cafe Updated" in refreshed
    assert "Tacoma" in refreshed


def test_csv_crm_client_raises_for_missing_record(tmp_path: Path) -> None:
    csv_path = tmp_path / "records.csv"
    _write_csv(
        csv_path,
        """BRIZO_ID,BUSINESS_NAME\nrow-1,Cafe Example\n""",
    )

    executor = CsvSQLExecutor(csv_path=csv_path, table_name="dataset")
    client = CsvCRMClient(executor=executor, primary_key="BRIZO_ID")

    try:
        client.update_record("missing", {"BUSINESS_NAME": "New"})
    except KeyError:
        pass
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected KeyError for unknown record")
