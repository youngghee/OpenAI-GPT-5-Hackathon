"""CSV-backed SQL executor supporting simple equality queries.

This lightweight engine loads a CSV dataset into memory and evaluates a narrow
subset of SQL statements. It is designed for prototyping the query bot without a
full database. Supported queries must match the pattern:

    SELECT <columns> FROM <table> WHERE <column> = '<value>' [LIMIT <n>];

- `<columns>` can be `*` or a comma-separated list of column names.
- Table and column names are matched case-insensitively against the CSV header.
- The optional `LIMIT` clause restricts the number of returned rows.

For unsupported statements, `NotImplementedError` is raised so higher layers can
fall back to alternative strategies.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_SELECT_RE = re.compile(
    r"^\s*select\s+(?P<columns>\*|[\w\s,]+)\s+from\s+(?P<table>\w+)\s+"
    r"where\s+(?P<where_col>\w+)\s*=\s*'(?P<where_val>[^']*)'"
    r"(?:\s+limit\s+(?P<limit>\d+))?\s*;?\s*$",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class CsvSQLExecutor:
    """Execute simple SQL statements by filtering an in-memory CSV dataset."""

    csv_path: str | Path
    table_name: str = "dataset"
    _rows: list[dict[str, Any]] = field(init=False, default_factory=list)
    _field_map: dict[str, str] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        path = Path(self.csv_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError("CSV file must include a header row")

            self._field_map = {name.lower(): name for name in reader.fieldnames}
            self._rows = list(reader)

    def run(self, statement: str) -> list[dict[str, Any]]:
        match = _SELECT_RE.match(statement)
        if not match:
            raise NotImplementedError("Only simple SELECT equality queries are supported")

        table = match.group("table")
        if table.lower() != self.table_name.lower():
            raise ValueError(f"Unknown table '{table}'. Expected '{self.table_name}'.")

        where_col = self._resolve_field(match.group("where_col"))
        where_val = match.group("where_val")
        limit = match.group("limit")
        selected_columns = self._resolve_columns(match.group("columns"))

        filtered = [row for row in self._rows if row.get(where_col) == where_val]

        if limit is not None:
            filtered = filtered[: int(limit)]

        if selected_columns is None:
            return filtered

        return [{column: row.get(column) for column in selected_columns} for row in filtered]

    def _resolve_field(self, name: str) -> str:
        resolved = self._field_map.get(name.lower())
        if resolved is None:
            raise KeyError(f"Column '{name}' not found in CSV header")
        return resolved

    def _resolve_columns(self, column_spec: str) -> list[str] | None:
        column_spec = column_spec.strip()
        if column_spec == "*":
            return None

        resolved = []
        for part in column_spec.split(","):
            column_name = part.strip()
            if not column_name:
                continue
            resolved.append(self._resolve_field(column_name))
        if not resolved:
            raise ValueError("No valid columns specified in SELECT clause")
        return resolved
