"""Scaffolding tests that document pending query agent behaviour."""

from __future__ import annotations

import pytest

from src.agents.query_agent import MissingDataFlagger, QueryAgent, SQLExecutor


class _SQLExecutorStub(SQLExecutor):
    def run(self, statement: str):  # type: ignore[override]
        return []


class _FlaggerStub(MissingDataFlagger):
    def flag_missing(self, ticket_id: str, question: str, facts):  # type: ignore[override]
        raise NotImplementedError


@pytest.mark.xfail(reason="QueryAgent implementation pending", raises=NotImplementedError)
def test_answer_question_not_implemented() -> None:
    agent = QueryAgent(sql_executor=_SQLExecutorStub(), missing_data_flagger=_FlaggerStub())

    agent.answer_question(ticket_id="T-1", question="Test?", record_id="123")
