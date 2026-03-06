from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from .utils import atomic_write_text


DEFAULT_CONFIG = {
    "paths": {
        "incoming_batch_folder": "incoming/batch",
        "processed_root_folder": "processed",
        "items_csv_path": "items.csv",
        "logs_folder": "logs",
    },
    "batch_rules": {
        "require_orders_csv": True,
        "orders_csv_glob": "*.csv",
        "label_pdf_glob": "*.pdf",
    },
    "print_layout": {
        "page_mode": "half_sheet_top",
        "overlay_mode": "margin",
        "placement_preset": "right_margin",
        "overlay_safe_rect": {"x": 350, "y": 48, "w": 230, "h": 700},
        "edge_inset_x": 8,
        "edge_inset_y": 24,
        "margin_box_width": 220,
        "margin_box_height": 700,
        "text_align": "left",
        "wrap_mode": "truncate",
        "orientation_mode": "normal",
        "rotated_primary_preset": "auto_perpendicular",
        "rotated_secondary_preset": "auto_perpendicular",
        "font_name": "Helvetica-Bold",
        "font_size": 16,
        "line_spacing": 20,
        "max_lines": 24,
        "max_title_length": 70,
        "show_field_labels": True,
        "overflow_to_backside": True,
        "overflow_mode": "backside",
        "secondary_anchor": "midline",
        "secondary_strip_height": 36,
        "secondary_strip_gap": 6,
        "field_order": ["label", "qty", "total", "location", "title"],
        "inline_fields_csv": "",
        "inline_separator": " | ",
    },
    "new_item_defaults": {
        "show_location": True,
        "show_label": True,
        "show_qty": True,
        "show_total_paid": True,
        "show_title": False,
    },
    "admin": {
        "archive_retention_days": 14,
    },
    "csv_mapping_cache": {
        "enable": True,
        "mapping_file": "csv_mapping.json",
    },
    "output_sort": {
        "mode": "processed",
        "priority_fields": ["label", "location", "qty", "item_key"],
        "enabled_fields": {
            "label": True,
            "qty": False,
            "item_key": False,
            "location": False,
        },
        "directions": {
            "label": "asc",
            "qty": "asc",
            "item_key": "asc",
            "location": "asc",
        },
    },
}


class SettingsManager:
    def __init__(self) -> None:
        self.base_dir = self._resolve_base_dir()
        self.config_path = self.base_dir / "config.yaml"
        self._config = self._load_or_create()
        self.ensure_directories()

    def _resolve_base_dir(self) -> Path:
        if getattr(sys, "frozen", False):
            return Path(sys.executable).resolve().parent
        return Path(__file__).resolve().parents[1]

    def _load_or_create(self) -> dict[str, Any]:
        if not self.config_path.exists():
            self.save(DEFAULT_CONFIG)
            return DEFAULT_CONFIG.copy()
        raw = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        merged = self._deep_merge(DEFAULT_CONFIG.copy(), raw)
        return merged

    def _deep_merge(self, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                base[k] = self._deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    def save(self, config: dict[str, Any]) -> None:
        self._config = config
        text = yaml.safe_dump(config, sort_keys=False)
        atomic_write_text(self.config_path, text)

    def ensure_directories(self) -> None:
        self.incoming_batch_folder.mkdir(parents=True, exist_ok=True)
        self.processed_root_folder.mkdir(parents=True, exist_ok=True)
        self.logs_folder.mkdir(parents=True, exist_ok=True)

    @property
    def incoming_batch_folder(self) -> Path:
        return self.base_dir / self.config["paths"]["incoming_batch_folder"]

    @property
    def processed_root_folder(self) -> Path:
        return self.base_dir / self.config["paths"]["processed_root_folder"]

    @property
    def logs_folder(self) -> Path:
        return self.base_dir / self.config["paths"]["logs_folder"]

    @property
    def items_csv_path(self) -> Path:
        return self.base_dir / self.config["paths"]["items_csv_path"]

    @property
    def mapping_file_path(self) -> Path:
        return self.base_dir / self.config["csv_mapping_cache"]["mapping_file"]

    def open_folder(self, path: Path) -> bool:
        target = path
        try:
            target.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        target_str = str(target.resolve())

        # `start` tends to bring Explorer to the foreground more reliably.
        try:
            subprocess.Popen(f'start "" "{target_str}"', shell=True)
            return True
        except Exception:
            pass

        try:
            os.startfile(target_str)  # type: ignore[attr-defined]
            return True
        except Exception:
            pass

        try:
            subprocess.Popen(["explorer", target_str])
            return True
        except Exception:
            return False
