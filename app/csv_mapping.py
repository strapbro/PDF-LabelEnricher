from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import atomic_write_json, load_json


class CsvMappingCache:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        return load_json(self.path, {})

    def save(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.path, payload)
