"""Unit tests for the theoretical in-memory SQL executor."""

from __future__ import annotations

from src.integrations.in_memory_sql_executor import InMemorySQLExecutor


def test_run_returns_canned_rows() -> None:
    executor = InMemorySQLExecutor(
        canned_results={
            "SELECT * FROM accounts WHERE id = '123'": [{"id": "123", "name": "Acme"}]
        }
    )

    rows = executor.run("SELECT * FROM accounts WHERE id = '123'")

    assert rows == [{"id": "123", "name": "Acme"}]


def test_run_defaults_to_empty_list() -> None:
    executor = InMemorySQLExecutor()

    rows = executor.run("SELECT * FROM opportunities")

    assert rows == []


def test_prime_adds_new_statement() -> None:
    executor = InMemorySQLExecutor()

    executor.prime("SELECT * FROM leads", [{"id": "L1"}])

    assert executor.run("SELECT * FROM leads") == [{"id": "L1"}]
