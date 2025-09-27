"""Scaffolding tests covering the runner entry point."""

from __future__ import annotations

import pytest

from src.core.runner import Runner


class _ScenarioLoaderStub:
    def load(self, profile: str):
        return []


@pytest.mark.xfail(reason="Runner implementation pending", raises=NotImplementedError)
def test_runner_execute_not_implemented() -> None:
    runner = Runner(scenario_loader=_ScenarioLoaderStub())

    runner.execute(profile="dev")
