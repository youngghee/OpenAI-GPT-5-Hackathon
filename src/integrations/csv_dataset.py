"""Utilities for inspecting CSV datasets used as the primary data source."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class CsvDatasetInspector:
    """Loads high-level metadata for the configured CSV export."""

    path: Path
    max_preview_rows: int = 5
    _columns: list[str] = field(init=False, default_factory=list)
    _preview_rows: list[dict[str, Any]] = field(init=False, default_factory=list)
    _row_count: int = field(init=False, default=0)

    def load(self) -> None:
        """Read header, preview sample, and row count."""

        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found at '{self.path}'")

        with self.path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError("CSV file must include a header row")
            self._columns = reader.fieldnames
            if all(field.strip().isdigit() for field in self._columns):
                raise ValueError("CSV header appears to be missing or numeric-only")
            for index, row in enumerate(reader):
                if index < self.max_preview_rows:
                    self._preview_rows.append(row)
                self._row_count += 1

    @property
    def columns(self) -> list[str]:
        if not self._columns:
            self.load()
        return self._columns

    @property
    def row_count(self) -> int:
        if self._row_count == 0:
            self.load()
        return self._row_count

    @property
    def preview_rows(self) -> list[dict[str, Any]]:
        if not self._preview_rows:
            self.load()
        return self._preview_rows

    def describe(self) -> dict[str, Any]:
        """Return a structured summary of the dataset."""

        return {
            "path": str(self.path),
            "row_count": self.row_count,
            "column_count": len(self.columns),
            "columns": self.columns,
            "preview_rows": self.preview_rows,
        }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect the configured CSV dataset")
    parser.add_argument("path", type=Path, help="Path to the CSV export")
    parser.add_argument(
        "--max-preview-rows",
        type=int,
        default=5,
        help="Number of rows to include in the preview output",
    )
    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()
    inspector = CsvDatasetInspector(path=args.path, max_preview_rows=args.max_preview_rows)
    summary = inspector.describe()
    print(summary)


if __name__ == "__main__":
    main()
