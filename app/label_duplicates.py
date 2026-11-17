"""Detect repeated physical labels before producing printable output."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import fitz


def label_fingerprint(path: Path, source: dict[str, Any]) -> str:
    with fitz.open(path) as document:
        page = document[0]
        clip = fitz.Rect(source["normalization_rect"]) if source.get("normalization_rect") else page.rect
        pixels = page.get_pixmap(matrix=fitz.Matrix(1, 1), clip=clip, colorspace=fitz.csGRAY, alpha=False)
        return hashlib.sha256(f"{pixels.width}x{pixels.height}:".encode() + pixels.samples).hexdigest()


def find_duplicate_labels(sources: list[dict[str, Any]], signals_for: Callable,
                          processed_root: Path, now: datetime | None = None) -> list[dict[str, Any]]:
    now = now or datetime.now()
    cutoff = now - timedelta(hours=24)
    seen: dict[str, dict[str, Any]] = {}
    for report_path in sorted(processed_root.glob("batch_*/batch_report.json"), reverse=True):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
            timestamp = report.get("timestamp", "")
            processed_at = datetime.strptime(timestamp, "%Y%m%d_%H%M%S_%f" if len(timestamp) > 15 else "%Y%m%d_%H%M%S")
            if not cutoff <= processed_at <= now:
                continue
            for row in report.get("results", []):
                if row.get("status") != "matched":
                    continue
                source = row.get("source", {})
                fingerprint = source.get("fingerprint", "")
                path = Path(row.get("label_pdf", ""))
                if not fingerprint and path.is_file():
                    fingerprint = label_fingerprint(path, source)
                tracking = re.sub(r"[^A-Z0-9]", "", str(source.get("tracking_number") or row.get("tracking_number") or "").upper())
                previous = {"previous_file": source.get("original_source", path.name),
                            "previous_page": source.get("page", 1),
                            "processed_at": processed_at.isoformat(sep=" ", timespec="seconds"),
                            "previous_batch": report_path.parent.name}
                for key in (["page:" + fingerprint] if fingerprint else []) + (["tracking:" + tracking] if tracking else []):
                    seen.setdefault(key, previous)
        except (OSError, ValueError, TypeError, KeyError) as error:
            raise ValueError(f"Could not check duplicate history in {report_path.parent.name}: {error}") from error

    duplicates: list[dict[str, Any]] = []
    for source in sources:
        path = Path(source["pdf"])
        signals = signals_for(path)
        source["fingerprint"] = label_fingerprint(path, source)
        source["tracking_number"] = signals.get("tracking_number", "")
        tracking = re.sub(r"[^A-Z0-9]", "", source["tracking_number"].upper())
        keys = ["page:" + source["fingerprint"]] + (["tracking:" + tracking] if tracking else [])
        previous = next((seen[key] for key in keys if key in seen), None)
        if previous:
            duplicates.append({"label_name": path.name, "file": source["original_source"],
                               "page": source.get("page", 1), "tracking": source["tracking_number"],
                               "recipient": signals.get("recipient_name", ""), **previous})
        else:
            for key in keys:
                seen[key] = {"previous_file": source["original_source"], "previous_page": source.get("page", 1),
                             "processed_at": "This batch", "previous_batch": "This batch"}
    return duplicates
