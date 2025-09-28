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
    _fieldnames: list[str] = field(init=False, default_factory=list)
    _path: Path = field(init=False)

    def __post_init__(self) -> None:
        self._path = Path(self.csv_path).expanduser()
        self.refresh()

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

    @property
    def columns(self) -> list[str]:
        """Return the original CSV column names."""

        return list(self._fieldnames)

    def refresh(self) -> None:
        """Reload the CSV contents from disk."""

        path = self._path
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError("CSV file must include a header row")
            fieldnames = list(reader.fieldnames)
            self._fieldnames = fieldnames
            self._field_map = {name.lower(): name for name in fieldnames}
            self._rows = [dict(row) for row in reader]

    def resolve_column(self, name: str) -> str:
        """Return the canonical column name for *name*."""

        return self._resolve_field(name)

    def apply_update(self, primary_key: str, record_id: str, updates: dict[str, Any]) -> bool:
        """Update a record in-memory and persist the CSV file."""

        if not updates:
            return False

        pk = self._resolve_field(primary_key)
        target_row: dict[str, Any] | None = None
        for row in self._rows:
            value = row.get(pk)
            if value is not None and str(value).strip() == str(record_id):
                target_row = row
                break

        if target_row is None:
            return False

        for column, value in updates.items():
            resolved = self._resolve_field(column)
            target_row[resolved] = "" if value is None else str(value)

        self._write_rows()
        self.refresh()
        return True

    def _write_rows(self) -> None:
        with self._path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self._fieldnames)
            writer.writeheader()
            for row in self._rows:
                writer.writerow(
                    {
                        field: "" if row.get(field) is None else str(row.get(field)).strip()
                        for field in self._fieldnames
                    }
                )

    def add_column(self, name: str, default: Any = "") -> None:
        """Add a column to the CSV dataset if it does not already exist."""

        if not name:
            raise ValueError("Column name must be provided")

        canonical = str(name)
        if canonical in self._fieldnames:
            return

        self._fieldnames.append(canonical)
        self._field_map[canonical.lower()] = canonical

        default_value = "" if default is None else str(default)
        for row in self._rows:
            row.setdefault(canonical, default_value)

        self._write_rows()
        self.refresh()
