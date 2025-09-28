"""CRM client implementation that persists updates back to the CSV dataset."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from src.agents.update_agent import CRMClient
from src.integrations.csv_sql_executor import CsvSQLExecutor


@dataclass(slots=True)
class CsvCRMClient(CRMClient):
    """Writes record updates back into the CSV source of truth."""

    executor: CsvSQLExecutor
    primary_key: str = "BRIZO_ID"
    history: list[dict[str, Any]] = field(init=False, default_factory=list)
    _lock: threading.Lock = field(init=False, repr=False, default_factory=threading.Lock)

    def update_record(self, record_id: str, payload: dict[str, Any]) -> None:  # type: ignore[override]
        if not payload:
            return

        with self._lock:
            resolved_updates: dict[str, Any] = {}
            for field, value in payload.items():
                column = self.executor.resolve_column(field)
                resolved_updates[column] = value

            applied = self.executor.apply_update(
                primary_key=self.primary_key,
                record_id=record_id,
                updates=resolved_updates,
            )

            if not applied:
                raise KeyError(f"Record '{record_id}' not found in CSV when applying updates")

            self.history.append({"record_id": record_id, "payload": dict(resolved_updates)})
