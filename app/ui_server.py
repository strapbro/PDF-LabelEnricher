from __future__ import annotations

import copy
import csv
import os
import re
import shutil
import threading
from difflib import SequenceMatcher
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import fitz
from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .batch_manager import BatchManager
from .item_db import ItemDB
from .label_text_extractor import extract_label_signals
from .overlay_renderer import create_overlay_pdf, get_page_size
from .pdf_merge import merge_overlays_on_first_page
from .settings_manager import SettingsManager
from .utils import setup_logging


settings = SettingsManager()
setup_logging(settings.logs_folder / "label_enricher.log")
batch_manager = BatchManager(settings)
item_db = ItemDB(settings.items_csv_path, settings.config.get("new_item_defaults", {}))


def _templates() -> Jinja2Templates:
    return Jinja2Templates(directory=str(settings.base_dir / "templates"))


def _tail_log(path: Path, lines: int = 50) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="ignore").splitlines()[-lines:]

def _open_file(path: Path) -> bool:
    try:
        os.startfile(str(path.resolve()))  # type: ignore[attr-defined]
        return True
    except Exception:
        return False


def _print_file(path: Path) -> bool:
    try:
        os.startfile(str(path.resolve()), "print")  # type: ignore[attr-defined]
        return True
    except Exception:
        return False



def _schedule_shutdown(delay_seconds: float = 0.8) -> None:
    def _shutdown() -> None:
        os._exit(0)

    threading.Timer(delay_seconds, _shutdown).start()


def _unresolved() -> list[dict[str, Any]]:
    return batch_manager._load_unresolved_queue()

def _human_reason(reason: str) -> str:
    r = (reason or "").strip().lower()
    if r == "ambiguous_or_low_confidence":
        return "Could not confidently match this label to one order."
    if r == "amazon_order_not_found_in_report":
        return "Amazon label order ID was not found in the uploaded Amazon report."
    if r.startswith("missing_required_fields:"):
        code = r.split(":", 1)[1]
        mapping = {
            "missing_items": "No items found for this order.",
            "missing_or_invalid_qty": "Quantity missing/invalid.",
            "missing_internal_label": "Internal label missing (and no usable title fallback).",
        }
        return mapping.get(code, f"Missing required field: {code}")
    if not r:
        return "Unknown reason."
    return reason

def _unresolved_for_ui() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in _unresolved():
        rr = dict(row)
        rr["reason_human"] = _human_reason(str(row.get("reason", "")))
        out.append(rr)
    return out


def _needs_review_count() -> int:
    rows = item_db.load_rows()
    return sum(1 for r in rows if str(r.get("needs_review", "0")).strip() == "1")


def _needs_review_rows(limit: int = 15) -> list[dict[str, Any]]:
    rows = item_db.load_rows()
    return [r for r in rows if str(r.get("needs_review", "0")).strip() == "1"][:limit]

def _queue_counts() -> tuple[int, int]:
    unresolved_count = len(_unresolved())
    review_count = _needs_review_count()
    return unresolved_count, review_count


def _queue_guard_redirect(action: str = "open_combined") -> RedirectResponse | None:
    unresolved_count, review_count = _queue_counts()
    if unresolved_count > 0:
        msg = f"Address+{unresolved_count}+unprocessed+label(s)+before+{action}."
        return RedirectResponse(url=f"/unprocessed?msg={msg}", status_code=303)
    if review_count > 0:
        msg = f"Address+{review_count}+items+needing+review+before+{action}."
        return RedirectResponse(url=f"/items/review?msg={msg}", status_code=303)
    return None


def _settings_changed_since_latest_batch() -> bool:
    snap = batch_manager.latest_batch_snapshot()
    batch_dir = Path(str(snap.get("batch_dir", ""))) if isinstance(snap, dict) and snap.get("batch_dir") else None
    if not batch_dir or not batch_dir.exists() or not settings.config_path.exists():
        return False
    try:
        return settings.config_path.stat().st_mtime > (batch_dir.stat().st_mtime + 0.5)
    except Exception:
        return False


def _items_link_targets(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        title = (row.get("item_title") or row.get("custom_label") or "").strip()
        pairs = [
            ("ebay", "ebay_item_number", (row.get("ebay_item_number") or "").strip()),
            ("amazon", "amazon_sku", (row.get("amazon_sku") or "").strip()),
            ("amazon", "amazon_asin", (row.get("amazon_asin") or "").strip().upper()),
            ("ebay", "item_id", (row.get("item_id") or "").strip() if (row.get("platform") or "").strip().lower() in ("ebay", "both") else ""),
            ("amazon", "item_id", (row.get("item_id") or "").strip().upper() if (row.get("platform") or "").strip().lower() in ("amazon", "both") else ""),
        ]
        for platform, id_type, id_value in pairs:
            if not id_value:
                continue
            key = (platform, id_type, id_value)
            if key in seen:
                continue
            seen.add(key)
            out.append({"platform": platform, "id_type": id_type, "id_value": id_value, "title": title})
    out.sort(key=lambda x: (x["platform"], x["id_type"], x["id_value"]))
    return out

def _items_backup_count() -> int:
    try:
        return len(list(item_db.backups_dir.glob("items_*.csv")))
    except Exception:
        return 0


def _label_hints_path() -> Path:
    return settings.base_dir / "label_location_hints.csv"

def _norm_hint_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())

def _load_label_hints() -> list[dict[str, str]]:
    p = _label_hints_path()
    if not p.exists():
        return []
    try:
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            keymap = {_norm_hint_header(h or ""): (h or "") for h in fieldnames}

            def pick(*aliases: str) -> str | None:
                for a in aliases:
                    h = keymap.get(_norm_hint_header(a))
                    if h is not None:
                        return h
                return None

            col_label = pick("internal label", "custom label", "label", "model", "item label")
            col_location = pick("location", "picking location")
            col_asin = pick("asin", "amazon asin", "amz asin")
            if col_label is None and fieldnames:
                col_label = fieldnames[0]
            if col_location is None and len(fieldnames) >= 2:
                col_location = fieldnames[1]
            if col_asin is None and len(fieldnames) >= 3:
                col_asin = fieldnames[2]

            out_by_label: dict[str, dict[str, str]] = {}
            for row in reader:
                label = (row.get(col_label, "") if col_label else "").strip()
                if not label:
                    continue
                location = (row.get(col_location, "") if col_location else "").strip()
                asin = (row.get(col_asin, "") if col_asin else "").strip().upper()
                key = label.lower()
                prev = out_by_label.get(key, {"label": label, "location": "", "asin": ""})
                if location and not prev.get("location"):
                    prev["location"] = location
                if asin and not prev.get("asin"):
                    prev["asin"] = asin
                if not prev.get("label"):
                    prev["label"] = label
                out_by_label[key] = prev
            out = list(out_by_label.values())
            out.sort(key=lambda x: (x.get("label", "") or "").lower())
            return out
    except Exception:
        return []

def _save_label_hints(rows: list[dict[str, str]]) -> int:
    clean: list[dict[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        label = str(r.get("label", "") or "").strip()
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        clean.append({
            "label": label,
            "location": str(r.get("location", "") or "").strip(),
            "asin": str(r.get("asin", "") or "").strip().upper(),
        })
    p = _label_hints_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["internal_label", "location", "asin"])
        writer.writeheader()
        for row in clean:
            writer.writerow({
                "internal_label": row["label"],
                "location": row["location"],
                "asin": row["asin"],
            })
    tmp.replace(p)
    return len(clean)

def _row_key(row: dict[str, Any]) -> str:
    parts = [
        (row.get("platform", "") or "").strip().lower(),
        (row.get("ebay_item_number", "") or "").strip(),
        (row.get("amazon_sku", "") or "").strip(),
        (row.get("amazon_asin", "") or "").strip().upper(),
        (row.get("item_id", "") or "").strip(),
    ]
    return "|".join(parts)

def _rows_with_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        rr["_row_key"] = _row_key(rr)
        out.append(rr)
    return out
def _unique_path(base_dir: Path, filename: str) -> Path:
    name = Path(filename).name or "upload.bin"
    candidate = base_dir / name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    i = 2
    while True:
        alt = base_dir / f"{stem}_{i}{suffix}"
        if not alt.exists():
            return alt
        i += 1


def _available_label_pdfs(extract_zip: bool = False) -> list[Path]:
    if extract_zip:
        batch_manager._extract_zip_files()
    files = [Path(p) for p in batch_manager.scan_inputs().get("files", []) if str(p).lower().endswith(".pdf")]
    labels = [p for p in files if "packing slip" not in p.name.lower()]
    labels.sort()
    return labels

def _manual_label_options() -> list[dict[str, str]]:
    # Include staged labels plus labels still in unresolved queue.
    values: set[str] = {str(p) for p in _available_label_pdfs(extract_zip=True)}
    unresolved_by_path: dict[str, str] = {}
    for row in _unresolved():
        p = str(row.get("label_pdf", "") or "").strip()
        if p:
            values.add(p)
            ident = str(row.get("label_identity", "") or "").strip()
            if ident:
                unresolved_by_path[p] = ident

    out: list[dict[str, str]] = []
    for s in sorted(values):
        p = Path(s)
        if not p.exists():
            continue

        display = unresolved_by_path.get(s, "")
        if not display:
            try:
                sig = extract_label_signals(p)
                display = batch_manager._build_label_identity(p, sig)
            except Exception:
                display = ""

        if not display:
            display = p.name
        label = p.name if display == p.name else f"{display} ({p.name})"
        out.append({"path": str(p), "label": label})
    return out



def _latest_reprocess_label_options() -> dict[str, Any]:
    data = batch_manager.latest_batch_reprocess_candidates()
    if not data.get("ok"):
        return {"ok": False, "error": data.get("error", "No previous batch archive found."), "batch_dir": "", "labels": []}

    archive_root_raw = str(data.get("archive_dir", "") or "").strip()
    archive_root = Path(archive_root_raw) if archive_root_raw else None

    rows: list[dict[str, str]] = []
    for rec in data.get("labels", []):
        order_id = str(rec.get("order_id", "") or "").strip()
        label_path = str(rec.get("label_pdf", "") or "").strip()
        p = Path(label_path) if label_path else None
        if (p is None or not p.exists()) and archive_root is not None and archive_root.exists() and label_path:
            archived_matches = sorted(archive_root.rglob(Path(label_path).name))
            if archived_matches:
                p = archived_matches[0]

        ship_name = str(rec.get("ship_name", "") or "").strip()
        ship_postal = str(rec.get("ship_postal", "") or "").strip()
        tracking = str(rec.get("tracking_number", "") or "").strip()

        ident_parts: list[str] = []
        if ship_name:
            ident_parts.append(ship_name)
        if ship_postal:
            ident_parts.append(f"ZIP {ship_postal}")
        if tracking:
            ident_parts.append(f"TRK {tracking}")
        ident = " | ".join(ident_parts)

        if not ident and p is not None and p.exists():
            try:
                sig = extract_label_signals(p)
                ident = batch_manager._build_label_identity(p, sig)
            except Exception:
                ident = ""

        if not ident:
            base = p.name if p is not None else "(unknown file)"
            ident = f"Order {order_id} ({base})" if order_id else base

        rows.append({
            "order_id": order_id,
            "platform": str(rec.get("platform", "") or "").strip(),
            "label_pdf": label_path,
            "label": ident,
        })

    rows.sort(key=lambda r: (r.get("label", "").lower(), r.get("order_id", "")))
    return {"ok": True, "error": "", "batch_dir": str(data.get("batch_dir", "")), "labels": rows}
def _to_float(value: str | float | int | None) -> float:
    if value is None:
        return 0.0
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _extract_manual_prefill_from_text(blob: str) -> dict[str, str]:
    text = str(blob or "")
    lower = text.lower()

    def _grab(pattern: str, flags: int = re.IGNORECASE) -> str:
        m = re.search(pattern, text, flags)
        return (m.group(1).strip() if m else "")

    order_id = _grab(r"\b(\d{3}-\d{7}-\d{7})\b")
    asin = _grab(r"\b(B[0-9A-Z]{9})\b")

    sku = _grab(r"\bsku\b\s*[:#-]?\s*([A-Z0-9._-]{2,64})")
    if not sku:
        m = re.search(r"\bsku\b\s*[:#-]?\s*\n\s*([A-Z0-9._-]{2,64})", text, re.IGNORECASE)
        sku = (m.group(1).strip() if m else "")

    ebay_item = _grab(r"\b(\d{10,14})\b")
    tracking = _grab(r"\b(1Z[0-9A-Z]{16}|9[0-9]{19,24}|[0-9]{12,22})\b")

    qty = _grab(r"\b(?:qty|quantity)\b\s*[:#-]?\s*(\d{1,4})")
    if not qty:
        qty = _grab(r"(?m)^\s*(\d{1,3})\s+\$?\d")
    if not qty:
        qty = "1"

    total = _grab(r"(?im)^\s*(?:grand total|item total)\s*[:$]*\s*\$?\s*([0-9]+(?:\.[0-9]{2})?)")
    if not total:
        total = _grab(r"\b(?:total|paid|amount)\b[^\d$]{0,8}\$?\s*([0-9]+(?:\.[0-9]{2})?)")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def _looks_like_title_line(ln: str) -> bool:
        ll = ln.strip().lower().strip(":")
        if len(ll) < 8:
            return False
        if re.search(r"\b(?:asin|sku|order item id|condition|tracking|order id|zip|qty|quantity)\b", ll):
            return False
        header_words = {
            "order contents",
            "status",
            "image",
            "product name",
            "more information",
            "unit price",
            "proceeds",
            "shipping address",
            "order date",
            "shipping service",
            "buyer name",
            "seller name",
            "order summary",
            "item subtotal",
            "item total",
            "grand total",
            "tax",
            "shipped",
            "package 1",
            "sales proceeds",
        }
        if ll in header_words:
            return False
        if re.fullmatch(r"\$?\d+(?:\.\d{2})?", ll):
            return False
        return True

    start_idx = 0
    for i, ln in enumerate(lines):
        if re.search(r"^\s*order contents\s*$", ln, re.IGNORECASE):
            start_idx = i
            break

    stop_idx = len(lines)
    for i in range(start_idx, len(lines)):
        ln = lines[i]
        if re.search(r"\b(?:asin|sku)\s*:", ln, re.IGNORECASE):
            stop_idx = i
            break

    search_lines = lines[start_idx:stop_idx] if stop_idx > start_idx else lines[:stop_idx]
    candidates = [ln for ln in search_lines if _looks_like_title_line(ln)]
    title = max(candidates, key=len).strip() if candidates else ""

    platform = "amazon" if ("amazon" in lower or asin or sku) else "ebay"
    item_key = sku or asin or ebay_item
    label_ref = tracking or order_id

    return {
        "platform": platform,
        "order_id": order_id,
        "item_key": item_key,
        "label_ref": label_ref,
        "item_asin": asin,
        "quantity": qty,
        "total_paid": total,
        "title": title,
        "custom_label": "",
        "location": "",
        "use_title_as_label": "1",
    }


def _split_manual_text_chunks(blob: str) -> list[str]:
    text = (blob or "").strip()
    if not text:
        return []

    order_matches = list(re.finditer(r"\b\d{3}-\d{7}-\d{7}\b", text))
    if len(order_matches) >= 2:
        chunks: list[str] = []
        for i, m in enumerate(order_matches):
            start = m.start()
            end = order_matches[i + 1].start() if (i + 1) < len(order_matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
        if chunks:
            return chunks

    marker_matches = list(re.finditer(r"(?im)^\s*order contents\s*$", text))
    if len(marker_matches) >= 2:
        chunks = []
        for i, m in enumerate(marker_matches):
            start = m.start()
            end = marker_matches[i + 1].start() if (i + 1) < len(marker_matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
        if chunks:
            return chunks

    parts = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
    return parts if parts else [text]
def _manual_batch_defaults(label_options: list[dict[str, str]], limit: int = 12) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for opt in label_options[: max(1, min(limit, len(label_options)))]:
        out.append(
            {
                "label_pdf": opt.get("path", ""),
                "platform": "amazon",
                "order_id": "",
                "item_key": "",
                "label_ref": "",
                "item_asin": "",
                "title": "",
                "custom_label": "",
                "quantity": "1",
                "total_paid": "",
                "location": "",
                "use_title_as_label": "1",
            }
        )
    if not out:
        out.append(
            {
                "label_pdf": "",
                "platform": "amazon",
                "order_id": "",
                "item_key": "",
                "label_ref": "",
                "item_asin": "",
                "title": "",
                "custom_label": "",
                "quantity": "1",
                "total_paid": "",
                "location": "",
                "use_title_as_label": "1",
            }
        )
    return out


def _manual_rows_from_form(form: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    prefixes: set[str] = set()
    for k in form.keys():
        m = re.match(r"^(row_\d+_)", str(k))
        if m:
            prefixes.add(m.group(1))

    for prefix in sorted(prefixes, key=lambda x: int(x.split("_")[1])):
        rows.append(
            {
                "label_pdf": str(form.get(prefix + "label_pdf", "") or "").strip(),
                "platform": str(form.get(prefix + "platform", "amazon") or "amazon").strip().lower(),
                "order_id": str(form.get(prefix + "order_id", "") or "").strip(),
                "item_key": str(form.get(prefix + "item_key", "") or "").strip(),
                "label_ref": str(form.get(prefix + "label_ref", "") or "").strip(),
                "item_asin": str(form.get(prefix + "item_asin", "") or "").strip().upper(),
                "title": str(form.get(prefix + "title", "") or "").strip(),
                "custom_label": str(form.get(prefix + "custom_label", "") or "").strip(),
                "quantity": str(form.get(prefix + "quantity", "1") or "1").strip(),
                "total_paid": str(form.get(prefix + "total_paid", "") or "").strip(),
                "location": str(form.get(prefix + "location", "") or "").strip(),
                "use_title_as_label": "1" if form.get(prefix + "use_title_as_label") else "0",
            }
        )
    return rows


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _manual_lookup_row(
    idx: dict[tuple[str, str], dict[str, str]],
    platform: str,
    key: str,
    asin: str,
) -> dict[str, str] | None:
    p = (platform or "").strip().lower()
    if p not in ("amazon", "ebay"):
        p = "amazon"
    for candidate in [key, asin]:
        c = (candidate or "").strip()
        if not c:
            continue
        row = idx.get((p, c))
        if row is not None:
            return row
    return None


TRACKING_REF_RE = re.compile(r"\b(1Z[0-9A-Z]{16}|9[0-9]{19,24}|[0-9]{12,22})\b", re.IGNORECASE)

def _looks_like_tracking_ref(value: str) -> bool:
    return bool(TRACKING_REF_RE.search(str(value or "").strip()))

def _has_letters(value: str) -> bool:
    return bool(re.search(r"[A-Za-z]", str(value or "")))

def _strong_name_match(a: str, b: str) -> bool:
    aa = str(a or "").strip()
    bb = str(b or "").strip()
    if not aa or not bb:
        return False
    if not _has_letters(aa) or not _has_letters(bb):
        return False
    return SequenceMatcher(None, aa.lower(), bb.lower()).ratio() >= 0.72

def _mark_item_needs_review(platform: str, key: str, asin: str, reason: str) -> None:
    p = (platform or "").strip().lower()
    if p not in ("amazon", "ebay"):
        return
    key_norm = (key or "").strip()
    asin_norm = (asin or "").strip().upper()
    wanted = {v for v in [key_norm, asin_norm] if v}
    if not wanted:
        return
    rows = item_db.load_rows()
    changed = False
    for row in rows:
        aliases = {
            str(row.get("item_id", "") or "").strip(),
            str(row.get("ebay_item_number", "") or "").strip(),
            str(row.get("amazon_sku", "") or "").strip(),
            str(row.get("amazon_asin", "") or "").strip().upper(),
        }
        aliases = {a for a in aliases if a}
        if wanted.intersection(aliases):
            row["needs_review"] = "1"
            row["needs_review_reason"] = reason
            changed = True
    if changed:
        item_db.save_rows(rows)

def _append_manual_unresolved(label_pdf: Path, reason: str) -> None:
    signals = extract_label_signals(label_pdf)
    queue = batch_manager._load_unresolved_queue()
    label_str = str(label_pdf)
    if any(str(q.get("label_pdf", "")) == label_str and str(q.get("reason", "")) == reason for q in queue):
        return
    queue.append({
        "label_pdf": label_str,
        "label_identity": batch_manager._build_label_identity(label_pdf, signals),
        "recipient_name": signals.get("recipient_name", ""),
        "tracking_number": signals.get("tracking_number", ""),
        "ship_postal": signals.get("ship_postal", ""),
        "reason": reason,
        "candidates": [],
    })
    batch_manager._save_unresolved_queue(queue)

def _manual_ebay_safety_ok(label_pdf: Path, label_ref: str) -> tuple[bool, str]:
    ref = str(label_ref or "").strip()
    if _looks_like_tracking_ref(ref):
        return True, "tracking_ref"
    signals = extract_label_signals(label_pdf)
    sig_tracking = str(signals.get("tracking_number", "") or "").strip()
    if _looks_like_tracking_ref(sig_tracking):
        return True, "label_tracking"
    sig_name = str(signals.get("recipient_name", "") or "").strip()
    if _strong_name_match(ref, sig_name):
        return True, "name_match"
    return False, "missing_tracking_or_weak_name_match"

def _detect_line_layout_mode(field_order_csv: str, inline_fields_csv: str, line_groups_csv: str = "") -> str:
    fo = (field_order_csv or "").replace(" ", "").lower()
    inf = (inline_fields_csv or "").replace(" ", "").lower()
    lg = (line_groups_csv or "").replace(" ", "").lower()
    if lg == "qty,label;total,location":
        return "qty_label_then_total_loc"
    if fo == "label,qty,total,location,title" and inf == "label,qty,total":
        return "label_qty_total_inline"
    if fo == "location,label,qty,total,title" and inf == "location,label,qty,total":
        return "location_label_qty_total_inline"
    if inf == "":
        return "stacked"
    return "custom"


def _line_groups_for_mode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m == "qty_label_then_total_loc":
        return "qty,label;total,location"
    return ""


def _apply_line_layout_mode(line_layout_mode: str, field_order_csv: str, inline_fields_csv: str) -> tuple[str, str]:
    mode = (line_layout_mode or "custom").strip().lower()
    if mode == "stacked":
        return "label,qty,total,location,title", ""
    if mode == "label_qty_total_inline":
        return "label,qty,total,location,title", "label,qty,total"
    if mode == "location_label_qty_total_inline":
        return "location,label,qty,total,title", "location,label,qty,total"
    if mode == "qty_label_then_total_loc":
        return "qty,label,total,location,title", ""
    return field_order_csv, inline_fields_csv


def _layout_ui_defaults() -> dict[str, Any]:
    layout = settings.config.get("print_layout", {})
    orientation = str(layout.get("orientation_mode", "normal"))
    primary = str(layout.get("rotated_primary_preset", layout.get("placement_preset", "top_margin")))
    secondary = str(layout.get("rotated_secondary_preset", "bottom_margin"))

    margin_direction = "top_bottom"
    if orientation == "rotated_90" and {primary, secondary} == {"left_margin", "right_margin"}:
        margin_direction = "left_right"

    field_order = layout.get("field_order", ["label", "qty", "total", "location", "title"])
    field_order_csv = field_order if isinstance(field_order, str) else ",".join([str(x) for x in field_order])
    inline_fields_csv = str(layout.get("inline_fields_csv", ""))
    line_groups_csv = str(layout.get("line_groups_csv", ""))

    strip_thickness = int(layout.get("margin_box_height", 36))
    if margin_direction == "left_right":
        strip_thickness = int(layout.get("margin_box_width", strip_thickness))

    return {
        "margin_direction": margin_direction,
        "margin_mode": "both" if str(layout.get("overflow_mode", "backside")) == "secondary_margin" else "single",
        "font_size": int(layout.get("font_size", 16)),
        "line_spacing": int(layout.get("line_spacing", 20)),
        "strip_thickness": strip_thickness,
        "edge_padding": int(layout.get("edge_inset_y", 8)),
        "side_padding": int(layout.get("edge_inset_x", 8)),
        "text_align": str(layout.get("text_align", "left")),
        "wrap_mode": str(layout.get("wrap_mode", "word")),
        "line_layout_mode": _detect_line_layout_mode(field_order_csv, inline_fields_csv, line_groups_csv),
        "field_order_csv": field_order_csv,
        "inline_fields_csv": inline_fields_csv,
        "inline_separator": str(layout.get("inline_separator", " | ")),
        "show_field_labels": bool(layout.get("show_field_labels", True)),
        "page_mode": str(layout.get("page_mode", "half_sheet_top")),
        "archive_retention_days": int(settings.config.get("admin", {}).get("archive_retention_days", 14)),
        "output_sort_mode": str(settings.config.get("output_sort", {}).get("mode", "processed")),
        "sort_priority_1": str((settings.config.get("output_sort", {}).get("priority_fields", ["label", "location", "qty", "item_key"]) + ["", "", "", ""])[0]),
        "sort_priority_2": str((settings.config.get("output_sort", {}).get("priority_fields", ["label", "location", "qty", "item_key"]) + ["", "", "", ""])[1]),
        "sort_priority_3": str((settings.config.get("output_sort", {}).get("priority_fields", ["label", "location", "qty", "item_key"]) + ["", "", "", ""])[2]),
        "sort_priority_4": str((settings.config.get("output_sort", {}).get("priority_fields", ["label", "location", "qty", "item_key"]) + ["", "", "", ""])[3]),
        "sort_enable_label": bool(settings.config.get("output_sort", {}).get("enabled_fields", {}).get("label", True)),
        "sort_enable_qty": bool(settings.config.get("output_sort", {}).get("enabled_fields", {}).get("qty", False)),
        "sort_enable_item_key": bool(settings.config.get("output_sort", {}).get("enabled_fields", {}).get("item_key", False)),
        "sort_enable_location": bool(settings.config.get("output_sort", {}).get("enabled_fields", {}).get("location", False)),
        "sort_dir_label": str(settings.config.get("output_sort", {}).get("directions", {}).get("label", "asc")),
        "sort_dir_qty": str(settings.config.get("output_sort", {}).get("directions", {}).get("qty", "asc")),
        "sort_dir_item_key": str(settings.config.get("output_sort", {}).get("directions", {}).get("item_key", "asc")),
        "sort_dir_location": str(settings.config.get("output_sort", {}).get("directions", {}).get("location", "asc")),
    }


def _build_preview_config(
    margin_direction: str,
    margin_mode: str,
    font_size: int,
    line_spacing: int,
    strip_thickness: int,
    edge_padding: int,
    side_padding: int,
    text_align: str,
    wrap_mode: str,
    line_layout_mode: str,
    field_order_csv: str,
    inline_fields_csv: str,
    inline_separator: str,
    show_field_labels: bool,
    page_mode: str,
    output_sort_mode: str = "processed",
    sort_priority_1: str = "label",
    sort_priority_2: str = "location",
    sort_priority_3: str = "qty",
    sort_priority_4: str = "item_key",
    sort_enable_label: bool = True,
    sort_enable_qty: bool = False,
    sort_enable_item_key: bool = False,
    sort_enable_location: bool = False,
    sort_dir_label: str = "asc",
    sort_dir_qty: str = "asc",
    sort_dir_item_key: str = "asc",
    sort_dir_location: str = "asc",
) -> dict[str, Any]:
    cfg = copy.deepcopy(settings.config)
    layout = cfg.setdefault("print_layout", {})

    layout["font_size"] = int(font_size)
    layout["line_spacing"] = int(line_spacing)
    layout["edge_inset_x"] = int(side_padding)
    layout["edge_inset_y"] = int(edge_padding)
    layout["text_align"] = str(text_align)
    layout["wrap_mode"] = str(wrap_mode)
    layout["inline_separator"] = str(inline_separator)
    layout["show_field_labels"] = bool(show_field_labels)
    layout["page_mode"] = page_mode

    if margin_direction == "left_right":
        layout["orientation_mode"] = "rotated_90"
        layout["rotated_primary_preset"] = "left_margin"
        layout["rotated_secondary_preset"] = "right_margin"
        layout["placement_preset"] = "left_margin"
        layout["margin_box_width"] = int(strip_thickness)
    else:
        layout["orientation_mode"] = "normal"
        layout["placement_preset"] = "top_margin"
        layout["margin_box_height"] = int(strip_thickness)

    field_order_csv, inline_fields_csv = _apply_line_layout_mode(line_layout_mode, field_order_csv, inline_fields_csv)
    layout["field_order"] = [x.strip() for x in (field_order_csv or "").split(",") if x.strip()]
    layout["inline_fields_csv"] = inline_fields_csv
    layout["line_groups_csv"] = _line_groups_for_mode(line_layout_mode)
    layout["overflow_mode"] = "secondary_margin" if margin_mode == "both" else "backside"

    allowed = {"label", "qty", "item_key", "location"}
    raw_priorities = [sort_priority_1, sort_priority_2, sort_priority_3, sort_priority_4]
    priorities: list[str] = []
    for f in raw_priorities:
        ff = str(f or "").strip().lower()
        if ff in allowed and ff not in priorities:
            priorities.append(ff)
    if not priorities:
        priorities = ["label", "location", "qty", "item_key"]

    cfg["output_sort"] = {
        "mode": str(output_sort_mode or "processed").strip().lower(),
        "priority_fields": priorities,
        "enabled_fields": {
            "label": bool(sort_enable_label),
            "qty": bool(sort_enable_qty),
            "item_key": bool(sort_enable_item_key),
            "location": bool(sort_enable_location),
        },
        "directions": {
            "label": "desc" if str(sort_dir_label).lower() == "desc" else "asc",
            "qty": "desc" if str(sort_dir_qty).lower() == "desc" else "asc",
            "item_key": "desc" if str(sort_dir_item_key).lower() == "desc" else "asc",
            "location": "desc" if str(sort_dir_location).lower() == "desc" else "asc",
        },
    }
    return cfg


def _sample_lines_for_order(field_order_csv: str, inline_fields_csv: str, show_field_labels: bool, inline_separator: str, line_groups_csv: str = "") -> list[str]:
    field_order_csv, inline_fields_csv = _apply_line_layout_mode("custom", field_order_csv, inline_fields_csv)
    tokens = [x.strip().lower() for x in (field_order_csv or "").split(",") if x.strip()]
    inline = {x.strip().lower() for x in (inline_fields_csv or "").split(",") if x.strip()}

    sample_map = {
        "label": "LABEL Ninja Bowl Lid",
        "qty": "QTY 2",
        "total": "TOTAL $26.98" if show_field_labels else "$26.98",
        "location": "LOC rack A-12",
        "title": "Ninja blender replacement bowl locking lid",
    }

    groups_raw = (line_groups_csv or "").strip().lower()
    if groups_raw:
        out: list[str] = []
        used: set[str] = set()
        for grp in groups_raw.split(";"):
            fields = [x.strip() for x in grp.split(",") if x.strip()]
            parts = [sample_map[f] for f in fields if f in sample_map]
            used.update([f for f in fields if f in sample_map])
            if parts:
                out.append((inline_separator or " | ").join(parts))
        rem = [sample_map[t] for t in tokens if t in sample_map and t not in used]
        out.extend(rem)
        return out

    out: list[str] = []
    run: list[str] = []
    for t in tokens:
        text = sample_map.get(t, "")
        if not text:
            continue
        if t in inline:
            run.append(text)
            continue
        if run:
            out.append((inline_separator or " | ").join(run))
            run = []
        out.append(text)
    if run:
        out.append((inline_separator or " | ").join(run))
    return out

def create_app() -> FastAPI:
    app = FastAPI(title="Label Enricher")

    app.mount("/static", StaticFiles(directory=str(settings.base_dir / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, msg: str = "", stale_open: int = 0):
        status = batch_manager.scan_inputs()
        latest_batch = batch_manager.latest_batch_snapshot()
        return _templates().TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "status": status,
                "message": msg,
                "log_lines": _tail_log(settings.logs_folder / "label_enricher.log"),
                "unresolved": _unresolved_for_ui(),
                "latest_batch": latest_batch,
                "needs_review_count": _needs_review_count(),
                "needs_review_rows": _needs_review_rows(),
                "stale_open": bool(stale_open),
            },
        )

    @app.post("/upload")
    async def upload(
        files: list[UploadFile] = File(...),
        quick_action: str = Form("upload_only"),
        auto_print_after_process: str | None = Form(None),
    ):
        saved = 0
        for file in files:
            dest = _unique_path(settings.incoming_batch_folder, file.filename)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                shutil.copyfileobj(file.file, f)
            saved += 1

        if quick_action != "upload_process_open":
            return RedirectResponse(url=f"/?msg=Uploaded+{saved}+file(s)+to+staging", status_code=303)

        result = batch_manager.process_batch()
        if not result.get("ok"):
            return RedirectResponse(url=f"/?msg=Uploaded+{saved}+file(s).+Process+failed:+{result.get('error', 'Batch failed')}", status_code=303)

        summary = result.get("report", {}).get("summary", {})
        msg = (
            f"Uploaded {saved} file(s). Batch complete. "
            f"Matched: {summary.get('matched', 0)}, unresolved: {summary.get('unresolved', 0)}, errors: {summary.get('errors', 0)}"
        )

        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok") and combined.get("path"):
                combined_path = Path(str(combined.get("path", "")))
                opened = _open_file(combined_path)
                if opened:
                    msg += f" | Opened combined PDF ({combined.get('count', 0)} files)"
                else:
                    msg += f" | Combined PDF ready ({combined.get('count', 0)} files)"

                if _parse_bool(auto_print_after_process, False):
                    if _print_file(combined_path):
                        msg += " | Sent combined PDF to default printer"
                    else:
                        msg += " | Auto-print failed (check default PDF app/printer)"
            else:
                msg += " | No combined PDF generated"

        if int(summary.get("processed_slips", 0) or 0) > 0:
            msg += f" | Packing slips synced: {summary.get('processed_slips', 0)} (items touched: {summary.get('synced_items', 0)})"

        return RedirectResponse(url=f"/?msg={msg}", status_code=303)

    @app.post("/staged/clear")
    def clear_staged_files():
        removed = batch_manager.clear_staged_files()
        return RedirectResponse(url=f"/?msg=Cleared+{removed}+staged+file(s)", status_code=303)

    @app.post("/process")
    def process_batch():
        result = batch_manager.process_batch()
        if not result.get("ok"):
            return RedirectResponse(url=f"/?msg={result.get('error', 'Batch failed')}", status_code=303)
        summary = result.get("report", {}).get("summary", {})
        msg = f"Batch complete. Matched: {summary.get('matched', 0)}, unresolved: {summary.get('unresolved', 0)}, errors: {summary.get('errors', 0)}"
        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok"):
                msg += f" | Combined PDF ready ({combined.get('count', 0)} files)"
        if int(summary.get("processed_slips", 0) or 0) > 0:
            msg += f" | Packing slips synced: {summary.get('processed_slips', 0)} (items touched: {summary.get('synced_items', 0)})"
        return RedirectResponse(url=f"/?msg={msg}", status_code=303)

    @app.post("/process/reprocess-latest")
    def reprocess_latest_batch():
        result = batch_manager.reprocess_latest_batch()
        if not result.get("ok"):
            return RedirectResponse(url=f"/?msg={result.get('error', 'Reprocess failed')}", status_code=303)

        summary = result.get("report", {}).get("summary", {})
        msg = f"Reprocessed previous batch. Restaged: {result.get('restaged_files', 0)} file(s). Matched: {summary.get('matched', 0)}, unresolved: {summary.get('unresolved', 0)}, errors: {summary.get('errors', 0)}"
        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok"):
                msg += f" | Combined PDF ready ({combined.get('count', 0)} files)"
        return RedirectResponse(url=f"/?msg={msg}", status_code=303)

    @app.get("/reprocess-select", response_class=HTMLResponse)
    def reprocess_select_page(request: Request, msg: str = ""):
        opts = _latest_reprocess_label_options()
        return _templates().TemplateResponse(
            "reprocess_select.html",
            {
                "request": request,
                "message": msg,
                "batch_dir": opts.get("batch_dir", ""),
                "rows": opts.get("labels", []),
                "error": opts.get("error", "") if not opts.get("ok") else "",
            },
        )

    @app.post("/process/reprocess-selected")
    async def reprocess_selected(request: Request):
        form = await request.form()
        selected_order_ids = [str(v) for v in form.getlist("selected_labels") if str(v).strip()]
        result = batch_manager.reprocess_selected_from_latest(selected_order_ids)
        if not result.get("ok"):
            return RedirectResponse(url=f"/reprocess-select?msg={result.get('error', 'Reprocess failed')}", status_code=303)

        summary = result.get("report", {}).get("summary", {})
        msg = (
            f"Reprocessed selected labels. Selected: {result.get('selected_labels', len(selected_order_ids))}. "
            f"Matched: {summary.get('matched', 0)}, unresolved: {summary.get('unresolved', 0)}, errors: {summary.get('errors', 0)}"
        )

        combined = result.get("combined", {})
        if isinstance(combined, dict) and combined.get("ok"):
            settings.open_folder(Path(str(combined.get("path", ""))))
            msg += f" | Opened combined PDF ({combined.get('count', 0)} files)"

        return RedirectResponse(url=f"/reprocess-select?msg={msg}", status_code=303)
    @app.post("/batch/combine-latest")
    def combine_latest():
        result = batch_manager.combine_latest_output_pdfs()
        if not result.get("ok"):
            return RedirectResponse(url=f"/?msg={result.get('error', 'Combine failed')}", status_code=303)
        return RedirectResponse(url=f"/?msg=Combined+{result.get('count', 0)}+PDFs:+{result.get('path', '')}", status_code=303)

    @app.post("/batch/open-combined-latest")
    def open_combined_latest(force_open: str | None = Form(None), reprocess_if_stale: str | None = Form(None)):
        guard = _queue_guard_redirect("opening+combined+pdf")
        if guard is not None:
            return guard

        stale = _settings_changed_since_latest_batch()
        if stale and not _parse_bool(force_open, False) and not _parse_bool(reprocess_if_stale, False):
            return RedirectResponse(
                url="/?msg=Layout+settings+changed+since+last+batch.+Reprocess+before+opening+for+accurate+output.&stale_open=1",
                status_code=303,
            )

        if stale and _parse_bool(reprocess_if_stale, False):
            processed = batch_manager.process_batch()
            if not processed.get("ok"):
                return RedirectResponse(url=f"/?msg=Reprocess+failed:+{processed.get('error', 'Batch failed')}", status_code=303)
            summary = processed.get("report", {}).get("summary", {})
            if int(summary.get("matched", 0) or 0) <= 0:
                return RedirectResponse(url="/?msg=Reprocess+completed+but+no+matched+labels+were+generated", status_code=303)

        snap = batch_manager.latest_batch_snapshot()
        path = snap.get("combined_pdf", "") if isinstance(snap, dict) else ""
        if not path or (stale and _parse_bool(reprocess_if_stale, False)):
            result = batch_manager.combine_latest_output_pdfs()
            if not result.get("ok"):
                return RedirectResponse(url=f"/?msg={result.get('error', 'Combine failed')}", status_code=303)
            path = result.get("path", "")

        if path:
            opened = _open_file(Path(path))
            if opened:
                return RedirectResponse(url=f"/?msg=Opened+combined+PDF:+{path}", status_code=303)
            return RedirectResponse(url=f"/?msg=Combined+PDF+ready+but+could+not+auto-open:+{path}", status_code=303)
        return RedirectResponse(url="/?msg=No+combined+PDF+available", status_code=303)

    @app.post("/open")
    def open_folder(target: str = Form(...)):
        ok = False
        if target == "incoming":
            ok = settings.open_folder(settings.incoming_batch_folder)
        elif target == "processed":
            snap = batch_manager.latest_batch_snapshot()
            latest = Path(snap["batch_dir"]) if isinstance(snap, dict) and snap.get("batch_dir") else settings.processed_root_folder
            ok = settings.open_folder(latest)

        if ok:
            return RedirectResponse(url="/?msg=Opened+folder", status_code=303)
        return RedirectResponse(url="/?msg=Could+not+open+folder+from+app", status_code=303)


    @app.post("/app/close", response_class=HTMLResponse)
    def close_app():
        _schedule_shutdown(0.8)
        return HTMLResponse(
            """
            <html>
              <head><title>Label Enricher Closing</title></head>
              <body style="font-family:Segoe UI,Arial,sans-serif;padding:24px;">
                <h2>Label Enricher is closing...</h2>
                <p>You can close this tab.</p>
              </body>
            </html>
            """
        )
    @app.get("/preview", response_class=HTMLResponse)
    def preview_page():
        return RedirectResponse(url="/settings", status_code=302)

    @app.get("/settings/live-preview.png")
    def settings_live_preview(
        label_pdf: str = Query(""),
        margin_direction: str = Query("top_bottom"),
        margin_mode: str = Query("both"),
        font_size: int = Query(14),
        line_spacing: int = Query(18),
        strip_thickness: int = Query(32),
        edge_padding: int = Query(8),
        side_padding: int = Query(8),
        text_align: str = Query("left"),
        wrap_mode: str = Query("word"),
        line_layout_mode: str = Query("qty_label_loc_inline"),
        field_order_csv: str = Query("label,qty,total,location,title"),
        inline_fields_csv: str = Query("qty,label,location"),
        inline_separator: str = Query(" | "),
        show_field_labels: str | None = Query("1"),
        page_mode: str = Query("half_sheet_top"),
    ):
        src = Path(label_pdf)
        if not src.exists():
            return Response(content=b"", media_type="image/png")

        cfg = _build_preview_config(
            margin_direction=margin_direction,
            margin_mode=margin_mode,
            font_size=font_size,
            line_spacing=line_spacing,
            strip_thickness=strip_thickness,
            edge_padding=edge_padding,
            side_padding=side_padding,
            text_align=text_align,
            wrap_mode=wrap_mode,
            line_layout_mode=line_layout_mode,
            field_order_csv=field_order_csv,
            inline_fields_csv=inline_fields_csv,
            inline_separator=inline_separator,
            show_field_labels=_parse_bool(show_field_labels, True),
            page_mode=page_mode,
        )

        field_order_csv, inline_fields_csv = _apply_line_layout_mode(line_layout_mode, field_order_csv, inline_fields_csv)
        lines = _sample_lines_for_order(field_order_csv, inline_fields_csv, _parse_bool(show_field_labels, True), inline_separator, _line_groups_for_mode(line_layout_mode))
        out_pdf = settings.processed_root_folder / "_live_preview.pdf"

        try:
            page_w, page_h = get_page_size(src)
            primary_overlay, remaining = create_overlay_pdf(page_w, page_h, lines, cfg, draw_rect=True, region="primary")
            overlays = [primary_overlay]
            if margin_mode == "both":
                secondary_lines = remaining if remaining else []
                secondary_overlay, still_remaining = create_overlay_pdf(page_w, page_h, secondary_lines, cfg, draw_rect=True, region="secondary")
                overlays.append(secondary_overlay)

                layout = cfg.get("print_layout", {})
                orientation = str(layout.get("orientation_mode", "normal"))
                primary_preset = str(layout.get("rotated_primary_preset", layout.get("placement_preset", "")))
                secondary_preset = str(layout.get("rotated_secondary_preset", ""))
                side_pair = {primary_preset, secondary_preset} == {"left_margin", "right_margin"}

                if still_remaining and orientation == "rotated_90" and side_pair:
                    spill_cfg = copy.deepcopy(cfg)
                    spill_layout = spill_cfg.setdefault("print_layout", {})
                    spill_layout["orientation_mode"] = "normal"
                    spill_layout["placement_preset"] = "top_margin"
                    spill_layout["margin_box_height"] = int(layout.get("secondary_strip_height", max(24, int(layout.get("margin_box_height", 36)))))

                    top_spill_overlay, still_remaining = create_overlay_pdf(page_w, page_h, still_remaining, spill_cfg, draw_rect=True, region="primary")
                    overlays.append(top_spill_overlay)

                    if still_remaining:
                        spill_layout["placement_preset"] = "bottom_margin"
                        bottom_spill_overlay, _ = create_overlay_pdf(page_w, page_h, still_remaining, spill_cfg, draw_rect=True, region="primary")
                        overlays.append(bottom_spill_overlay)

            merge_overlays_on_first_page(src, overlays, out_pdf)

            with fitz.open(str(out_pdf)) as doc:
                pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
                return Response(content=pix.tobytes("png"), media_type="image/png")
        except Exception:
            return Response(content=b"", media_type="image/png")

    @app.get("/items", response_class=HTMLResponse)
    def items_page(request: Request, msg: str = ""):
        rows = _rows_with_keys(item_db.load_rows())
        return _templates().TemplateResponse("items.html", {"request": request, "rows": rows, "message": msg, "backups_count": _items_backup_count(), "link_targets": _items_link_targets(rows), "page_mode": "items", "label_hints": _load_label_hints()})

    @app.get("/items/review", response_class=HTMLResponse)
    def items_review_page(request: Request, msg: str = ""):
        rows = _rows_with_keys([r for r in item_db.load_rows() if str(r.get("needs_review", "0")).strip() == "1"])
        return _templates().TemplateResponse("items.html", {"request": request, "rows": rows, "message": msg, "backups_count": _items_backup_count(), "link_targets": _items_link_targets(rows), "page_mode": "review", "label_hints": _load_label_hints()})

    @app.post("/items/save")
    async def items_save(request: Request):
        form = await request.form()
        try:
            kept, deleted = item_db.update_rows_from_form(dict(form))
            return RedirectResponse(url=f"/items?msg=Saved+{kept}+row(s);+deleted+{deleted}+row(s)", status_code=303)
        except PermissionError:
            return RedirectResponse(
                url="/items?msg=Could+not+save+items.csv.+Please+close+items.csv+in+Excel+and+try+again",
                status_code=303,
            )
        except OSError as exc:
            return RedirectResponse(url=f"/items?msg=Could+not+save+items.csv:+{type(exc).__name__}", status_code=303)

    @app.get("/items/export")
    def items_export():
        if not settings.items_csv_path.exists():
            return RedirectResponse(url="/items?msg=items.csv+not+found", status_code=303)
        return FileResponse(path=str(settings.items_csv_path), filename="items.csv", media_type="text/csv")

    @app.post("/items/backup")
    def items_backup_now(from_page: str = Form("items")):
        backup = item_db.create_backup_now()
        if backup:
            msg = quote_plus(f"Backup created: {backup.name} in {backup.parent}")
        else:
            msg = "No+backup+created"
        if from_page == "dashboard":
            return RedirectResponse(url=f"/?msg={msg}", status_code=303)
        return RedirectResponse(url=f"/items?msg={msg}", status_code=303)

    @app.post("/items/open-backups")
    def items_open_backups(from_page: str = Form("items")):
        ok = settings.open_folder(item_db.backups_dir)
        msg = "Opened+items+backups+folder" if ok else "Could+not+open+backups+folder"
        if from_page == "dashboard":
            return RedirectResponse(url=f"/?msg={msg}", status_code=303)
        return RedirectResponse(url=f"/items?msg={msg}", status_code=303)

    @app.post("/items/clear-needs-review")
    def items_clear_needs_review(from_page: str = Form("dashboard")):
        cleared = item_db.clear_needs_review()
        if from_page == "items":
            return RedirectResponse(url=f"/items?msg=Cleared+{cleared}+needs-review+item(s)", status_code=303)
        return RedirectResponse(url=f"/?msg=Cleared+{cleared}+needs-review+item(s)", status_code=303)

    @app.post("/items/sync")
    async def items_sync(master_csv: UploadFile = File(...)):
        temp = settings.incoming_batch_folder / f"_sync_{master_csv.filename}"
        with temp.open("wb") as f:
            shutil.copyfileobj(master_csv.file, f)
        changed = item_db.sync_from_master_csv(temp)
        try:
            temp.unlink(missing_ok=True)
        except Exception:
            pass
        return RedirectResponse(url=f"/items?msg=Synced+{changed}+updates", status_code=303)


    @app.post("/hints/upload")
    async def hints_upload(from_page: str = Form("items"), hints_csv: UploadFile = File(...)):
        temp = settings.incoming_batch_folder / f"_hints_{hints_csv.filename}"
        with temp.open("wb") as f:
            shutil.copyfileobj(hints_csv.file, f)
        count = 0
        try:
            with temp.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                keymap = {_norm_hint_header(h or ""): (h or "") for h in fieldnames}
                def pick(*aliases: str) -> str | None:
                    for a in aliases:
                        h = keymap.get(_norm_hint_header(a))
                        if h is not None:
                            return h
                    return None
                col_label = pick("internal label", "custom label", "label", "model", "item label")
                col_location = pick("location", "picking location")
                col_asin = pick("asin", "amazon asin", "amz asin")
                if col_label is None and fieldnames:
                    col_label = fieldnames[0]
                if col_location is None and len(fieldnames) >= 2:
                    col_location = fieldnames[1]
                if col_asin is None and len(fieldnames) >= 3:
                    col_asin = fieldnames[2]
                rows: list[dict[str, str]] = []
                for r in reader:
                    rows.append({
                        "label": (r.get(col_label, "") if col_label else "").strip(),
                        "location": (r.get(col_location, "") if col_location else "").strip(),
                        "asin": (r.get(col_asin, "") if col_asin else "").strip().upper(),
                    })
                count = _save_label_hints(rows)
        except Exception:
            count = 0
        finally:
            try:
                temp.unlink(missing_ok=True)
            except Exception:
                pass
        url = "/items" if from_page == "items" else "/manual-entry"
        return RedirectResponse(url=f"{url}?msg=Saved+{count}+label/location+hint(s)", status_code=303)

    @app.post("/hints/clear")
    def hints_clear(from_page: str = Form("items")):
        try:
            _label_hints_path().unlink(missing_ok=True)
            msg = "Cleared+label/location+hints"
        except Exception:
            msg = "Could+not+clear+label/location+hints"
        url = "/items" if from_page == "items" else "/manual-entry"
        return RedirectResponse(url=f"{url}?msg={msg}", status_code=303)

    def _render_manual_entry(request: Request, msg: str = "", prefill: dict[str, Any] | None = None):
        label_options = _manual_label_options()
        first_label = label_options[0].get("path", "") if label_options else ""
        defaults: dict[str, Any] = {
            "label_pdf": first_label,
            "platform": "amazon",
            "order_id": "",
            "item_key": "",
            "label_ref": "",
            "item_asin": "",
            "title": "",
            "custom_label": "",
            "quantity": "1",
            "total_paid": "",
            "location": "",
            "use_title_as_label": "1",
            "messy_text": "",
            "batch_messy_text": "",
            "write_to_items": "0",
            "batch_entries": _manual_batch_defaults(label_options),
        }
        if prefill:
            defaults.update({k: v for k, v in prefill.items() if v is not None})
            if not defaults.get("batch_entries"):
                defaults["batch_entries"] = _manual_batch_defaults(label_options)

        if not str(defaults.get("label_pdf", "") or "").strip() and first_label:
            defaults["label_pdf"] = first_label

        return _templates().TemplateResponse(
            "manual_entry.html",
            {
                "request": request,
                "message": msg,
                "labels": label_options,
                "unresolved_count": len(_unresolved()),
                "prefill": defaults,
                "label_hints": _load_label_hints(),
            },
        )
    @app.get("/manual-entry", response_class=HTMLResponse)
    def manual_entry_page(request: Request, msg: str = ""):
        return _render_manual_entry(request, msg=msg)

    @app.post("/manual-entry/upload")
    async def manual_entry_upload(files: list[UploadFile] = File(...)):
        saved = 0
        for file in files:
            dest = _unique_path(settings.incoming_batch_folder, file.filename)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                shutil.copyfileobj(file.file, f)
            saved += 1
        return RedirectResponse(url=f"/manual-entry?msg=Uploaded+{saved}+file(s)+to+incoming/batch", status_code=303)

    @app.post("/manual-entry/parse-text", response_class=HTMLResponse)
    async def manual_entry_parse_text(request: Request, messy_text: str = Form(""), label_pdf: str = Form("")):
        prefill = _extract_manual_prefill_from_text(messy_text)
        prefill["messy_text"] = messy_text or ""
        if label_pdf:
            prefill["label_pdf"] = label_pdf
        return _render_manual_entry(request, msg="Parsed text and pre-filled fields. Verify before creating output.", prefill=prefill)

    @app.post("/manual-entry/parse-text-batch", response_class=HTMLResponse)
    async def manual_entry_parse_text_batch(request: Request):
        form = await request.form()
        blob = str(form.get("batch_messy_text", "") or "")
        rows = _manual_rows_from_form(dict(form))
        chunks = _split_manual_text_chunks(blob)
        parsed = [_extract_manual_prefill_from_text(ch) for ch in chunks]

        batch_entries: list[dict[str, str]] = []
        for i, row in enumerate(rows):
            base = dict(row)
            if i < len(parsed):
                p = parsed[i]
                for key in ["platform", "order_id", "item_key", "label_ref", "item_asin", "title", "quantity", "total_paid"]:
                    val = str(p.get(key, "") or "").strip()
                    if val:
                        base[key] = val
            if not str(base.get("use_title_as_label", "")).strip():
                base["use_title_as_label"] = "1"
            batch_entries.append(base)

        prefill: dict[str, Any] = {
            "batch_messy_text": blob,
            "batch_entries": batch_entries,
            "write_to_items": "0",
        }
        return _render_manual_entry(request, msg=f"Parsed {len(parsed)} text chunk(s) into {len(batch_entries)} manual row(s). Verify before creating.", prefill=prefill)

    @app.post("/manual-entry/create-batch")
    async def manual_entry_create_batch(request: Request):
        guard = _queue_guard_redirect("creating+manual+output")
        if guard is not None:
            return guard
        form = await request.form()
        rows = _manual_rows_from_form(dict(form))
        write_to_items = bool(form.get("write_to_items"))

        if not rows:
            return RedirectResponse(url="/manual-entry?msg=No+manual+rows+to+process", status_code=303)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = settings.processed_root_folder / "manual_entries" / f"manual_batch_{ts}"
        out_dir.mkdir(parents=True, exist_ok=True)

        idx = item_db.index()
        rendered: list[Path] = []
        errors: list[str] = []

        for i, r in enumerate(rows, start=1):
            label_pdf = str(r.get("label_pdf", "") or "").strip()
            if not label_pdf:
                continue

            src = Path(label_pdf)
            if not src.exists():
                name = src.name
                for p in settings.processed_root_folder.rglob(name):
                    if "input_archive" in p.parts:
                        src = p
                        break
            if not src.exists():
                errors.append(f"Row {i}: Label PDF not found")
                continue

            platform = str(r.get("platform", "amazon") or "amazon").strip().lower()
            if platform not in ("amazon", "ebay"):
                platform = "amazon"

            key = str(r.get("item_key", "") or "").strip()
            label_ref = str(r.get("label_ref", "") or "").strip()
            if not label_ref:
                errors.append(f"Row {i}: Label link reference required")
                continue

            asin = str(r.get("item_asin", "") or "").strip().upper()
            if platform == "amazon" and not asin and key.upper().startswith("B") and len(key) == 10:
                asin = key.upper()

            if platform == "ebay":
                ok, why = _manual_ebay_safety_ok(src, label_ref)
                if not ok:
                    reason = f"manual_ebay_safety:{why}"
                    _mark_item_needs_review(platform, key, asin, reason)
                    _append_manual_unresolved(src, reason)
                    errors.append(f"Row {i}: Manual eBay safety check failed (tracking missing and name is not a strong match)")
                    continue


            try:
                qty = int(float(str(r.get("quantity", "1") or "1")))
            except Exception:
                qty = 0
            if qty <= 0:
                errors.append(f"Row {i}: Quantity invalid")
                continue


            title = str(r.get("title", "") or "").strip()
            label = str(r.get("custom_label", "") or "").strip()
            db_row = _manual_lookup_row(idx, platform, key, asin) if key else None
            if not label and db_row is not None:
                label = str(db_row.get("custom_label", "") or "").strip() or str(db_row.get("item_title", "") or "").strip()
                if not title:
                    title = str(db_row.get("item_title", "") or "").strip()
                if not str(r.get("location", "") or "").strip():
                    r["location"] = str(db_row.get("location", "") or "").strip()

            if not label and str(r.get("use_title_as_label", "1") or "1") != "0" and title:
                label = title

            if not label and not key:
                errors.append(f"Row {i}: Provide internal label or item key")
                continue
            if not label:
                errors.append(f"Row {i}: Could not derive internal label from key/title")
                continue

            if write_to_items and key:
                row = item_db.ensure_item(platform, key, title=title, item_asin=asin)
                all_rows = item_db.load_rows()
                for rr in all_rows:
                    aliases = {
                        str(rr.get("item_id", "") or "").strip(),
                        str(rr.get("ebay_item_number", "") or "").strip(),
                        str(rr.get("amazon_sku", "") or "").strip(),
                        str(rr.get("amazon_asin", "") or "").strip(),
                    }
                    aliases = {a for a in aliases if a}
                    if key in aliases or (asin and asin in aliases) or rr is row:
                        rr["custom_label"] = label
                        if str(r.get("location", "") or "").strip():
                            rr["location"] = str(r.get("location", "") or "").strip()
                        rr["needs_review"] = "0"
                        if title and (platform == "amazon" or not str(rr.get("item_title", "")).strip()):
                            rr["item_title"] = title
                        break
                item_db.save_rows(all_rows)
                idx = item_db.index()

            order_id = str(r.get("order_id", "") or "").strip() or f"MANUAL-{ts}-{i:02d}"
            total_val = _to_float(str(r.get("total_paid", "") or ""))
            location = str(r.get("location", "") or "").strip()

            manual_row: dict[str, str] = {
                "platform": platform,
                "ebay_item_number": key if platform == "ebay" else "",
                "amazon_sku": key if (platform == "amazon" and not (key.upper().startswith("B") and len(key) == 10)) else "",
                "amazon_asin": asin,
                "item_id": key,
                "item_title": title,
                "custom_label": label,
                "location": location,
                "show_label": "1",
                "show_qty": "1",
                "show_total_paid": "1" if str(r.get("total_paid", "") or "").strip() else "0",
                "show_title": "1" if title else "0",
                "show_location": "1" if location else "0",
                "needs_review": "0",
                "needs_review_reason": "",
                "last_seen": "",
            }

            item: dict[str, Any] = {
                "item_id": key,
                "title": title or label,
                "quantity": qty,
                "line_total": total_val,
                "_manual_row": manual_row,
            }
            if platform == "amazon":
                item["item_sku"] = key if not (key.upper().startswith("B") and len(key) == 10) else ""
                item["item_asin"] = asin or (key.upper() if key.upper().startswith("B") and len(key) == 10 else "")
            else:
                item["ebay_item_number"] = key

            order = {
                "platform": platform,
                "order_id": order_id,
                "ship_name": label_ref,
                "ship_postal": "",
                "tracking_number": label_ref,
                "items": [item],
                "total_paid": total_val,
                "source": "manual_entry_batch",
            }

            out_pdf = batch_manager._render_one_label(src, order, idx, out_dir, auto_add_missing_items=False)
            rendered.append(out_pdf)
            batch_manager.remove_unresolved_entry(str(Path(label_pdf)))

        if rendered:
            from pypdf import PdfReader, PdfWriter
            writer = PdfWriter()
            for pdf_path in rendered:
                try:
                    rpdf = PdfReader(str(pdf_path))
                    for pg in rpdf.pages:
                        writer.add_page(pg)
                except Exception:
                    errors.append(f"Combine skipped: {pdf_path.name}")
            combined = out_dir / "combined_manual_output.pdf"
            with combined.open("wb") as f:
                writer.write(f)
            settings.open_folder(combined)

        msg = f"Manual batch complete. Generated: {len(rendered)}"
        if errors:
            preview = "; ".join(errors[:3])
            if len(errors) > 3:
                preview += f"; (+{len(errors)-3} more)"
            msg += f" | Errors: {len(errors)} | {preview}"
        if rendered:
            msg += " | Opened combined manual PDF"
        return RedirectResponse(url=f"/manual-entry?msg={msg}", status_code=303)

    @app.post("/manual-entry/create")
    async def manual_entry_create(
        label_pdf: str = Form(...),
        platform: str = Form("amazon"),
        order_id: str = Form(""),
        item_key: str = Form(""),
        item_asin: str = Form(""),
        label_ref: str = Form(""),
        title: str = Form(""),
        custom_label: str = Form(""),
        quantity: int = Form(1),
        total_paid: str = Form(""),
        location: str = Form(""),
        use_title_as_label: str | None = Form(None),
        write_to_items: str | None = Form(None),
    ):
        guard = _queue_guard_redirect("creating+manual+output")
        if guard is not None:
            return guard

        src = Path(label_pdf)
        if not src.exists():
            name = src.name
            for p in settings.processed_root_folder.rglob(name):
                if "input_archive" in p.parts:
                    src = p
                    break

        if not src.exists():
            return RedirectResponse(url="/manual-entry?msg=Label+PDF+not+found", status_code=303)

        p = (platform or "").strip().lower()
        if p not in ("amazon", "ebay"):
            p = "amazon"

        key = (item_key or "").strip()
        ref = (label_ref or "").strip()
        if not ref:
            return RedirectResponse(url="/manual-entry?msg=Label+link+reference+required+(name/tracking/order+ref)", status_code=303)

        if int(quantity or 0) <= 0:
            return RedirectResponse(url="/manual-entry?msg=Quantity+must+be+1+or+higher", status_code=303)

        asin = (item_asin or "").strip().upper()
        if p == "amazon" and not asin and key.upper().startswith("B") and len(key) == 10:
            asin = key.upper()


        if p == "ebay":
            ok, why = _manual_ebay_safety_ok(src, ref)
            if not ok:
                reason = f"manual_ebay_safety:{why}"
                _mark_item_needs_review(p, key, asin, reason)
                _append_manual_unresolved(src, reason)
                return RedirectResponse(
                    url="/manual-entry?msg=Manual+eBay+safety+check+failed:+tracking+missing+and+name+match+too+weak.+Sent+to+review.",
                    status_code=303,
                )

        t = (title or "").strip()
        label = (custom_label or "").strip()
        db_row = _manual_lookup_row(item_db.index(), p, key, asin) if key else None
        if not label and db_row is not None:
            label = str(db_row.get("custom_label", "") or "").strip() or str(db_row.get("item_title", "") or "").strip()
            if not t:
                t = str(db_row.get("item_title", "") or "").strip()
            if not (location or "").strip():
                location = str(db_row.get("location", "") or "").strip()

        if not label and use_title_as_label and t:
            label = t

        if not label and not key:
            return RedirectResponse(url="/manual-entry?msg=Provide+internal+label+or+item+key", status_code=303)
        if not label:
            return RedirectResponse(url="/manual-entry?msg=Could+not+derive+internal+label+from+key/title", status_code=303)

        if write_to_items and key:
            row = item_db.ensure_item(p, key, title=t, item_asin=asin)
            rows = item_db.load_rows()
            for r in rows:
                aliases = {
                    str(r.get("item_id", "") or "").strip(),
                    str(r.get("ebay_item_number", "") or "").strip(),
                    str(r.get("amazon_sku", "") or "").strip(),
                    str(r.get("amazon_asin", "") or "").strip(),
                }
                aliases = {a for a in aliases if a}
                if key in aliases or (asin and asin in aliases) or r is row:
                    r["custom_label"] = label
                    if location:
                        r["location"] = location.strip()
                    r["needs_review"] = "0"
                    if t and (p == "amazon" or not str(r.get("item_title", "")).strip()):
                        r["item_title"] = t
                    break
            item_db.save_rows(rows)

        manual_order_id = (order_id or "").strip() or f"MANUAL-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        total_val = _to_float(total_paid)
        loc = (location or "").strip()
        manual_row: dict[str, str] = {
            "platform": p,
            "ebay_item_number": key if p == "ebay" else "",
            "amazon_sku": key if (p == "amazon" and not (key.upper().startswith("B") and len(key) == 10)) else "",
            "amazon_asin": asin,
            "item_id": key,
            "item_title": t,
            "custom_label": label,
            "location": loc,
            "show_label": "1",
            "show_qty": "1",
            "show_total_paid": "1" if str(total_paid or "").strip() else "0",
            "show_title": "1" if t else "0",
            "show_location": "1" if loc else "0",
            "needs_review": "0",
            "needs_review_reason": "",
            "last_seen": "",
        }

        item: dict[str, Any] = {
            "item_id": key,
            "title": t or label,
            "quantity": int(quantity),
            "line_total": total_val,
            "_manual_row": manual_row,
        }
        if p == "amazon":
            item["item_sku"] = key if not (key.upper().startswith("B") and len(key) == 10) else ""
            item["item_asin"] = asin or (key.upper() if key.upper().startswith("B") and len(key) == 10 else "")
        else:
            item["ebay_item_number"] = key

        order = {
            "platform": p,
            "order_id": manual_order_id,
            "ship_name": ref,
            "ship_postal": "",
            "tracking_number": ref,
            "items": [item],
            "total_paid": total_val,
            "source": "manual_entry",
        }

        out_dir = settings.processed_root_folder / "manual_entries" / f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        out_dir.mkdir(parents=True, exist_ok=True)
        _out_pdf = batch_manager._render_one_label(src, order, item_db.index(), out_dir, auto_add_missing_items=False)
        opened = settings.open_folder(_out_pdf)

        batch_manager.remove_unresolved_entry(str(Path(label_pdf)))
        msg = "Created+manual+output+PDF"
        if opened:
            msg += "+and+opened+it"
        else:
            msg += "+(could+not+auto-open)"
        return RedirectResponse(url=f"/manual-entry?msg={msg}", status_code=303)
    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(request: Request, msg: str = ""):
        labels = [str(p) for p in _available_label_pdfs(extract_zip=True)]
        ui = _layout_ui_defaults()
        return _templates().TemplateResponse(
            "settings.html",
            {
                "request": request,
                "config": settings.config,
                "message": msg,
                "labels": labels,
                "ui": ui,
            },
        )

    @app.post("/settings/save")
    async def settings_save(
        margin_direction: str = Form("top_bottom"),
        margin_mode: str = Form("both"),
        font_size: int = Form(14),
        line_spacing: int = Form(18),
        strip_thickness: int = Form(32),
        edge_padding: int = Form(8),
        side_padding: int = Form(8),
        text_align: str = Form("left"),
        wrap_mode: str = Form("word"),
        line_layout_mode: str = Form("qty_label_loc_inline"),
        field_order_csv: str = Form("label,qty,total,location,title"),
        inline_fields_csv: str = Form("qty,label,location"),
        inline_separator: str = Form(" | "),
        show_field_labels: str | None = Form(None),
        page_mode: str = Form("half_sheet_top"),
        archive_retention_days: int = Form(14),
        output_sort_mode: str = Form("processed"),
        sort_priority_1: str = Form("label"),
        sort_priority_2: str = Form("location"),
        sort_priority_3: str = Form("qty"),
        sort_priority_4: str = Form("item_key"),
        sort_enable_label: str | None = Form(None),
        sort_enable_qty: str | None = Form(None),
        sort_enable_item_key: str | None = Form(None),
        sort_enable_location: str | None = Form(None),
        sort_dir_label: str = Form("asc"),
        sort_dir_qty: str = Form("asc"),
        sort_dir_item_key: str = Form("asc"),
        sort_dir_location: str = Form("asc"),
    ):
        cfg = _build_preview_config(
            margin_direction=margin_direction,
            margin_mode=margin_mode,
            font_size=font_size,
            line_spacing=line_spacing,
            strip_thickness=strip_thickness,
            edge_padding=edge_padding,
            side_padding=side_padding,
            text_align=text_align,
            wrap_mode=wrap_mode,
            line_layout_mode=line_layout_mode,
            field_order_csv=field_order_csv,
            inline_fields_csv=inline_fields_csv,
            inline_separator=inline_separator,
            show_field_labels=bool(show_field_labels),
            page_mode=page_mode,
            output_sort_mode=output_sort_mode,
            sort_priority_1=sort_priority_1,
            sort_priority_2=sort_priority_2,
            sort_priority_3=sort_priority_3,
            sort_priority_4=sort_priority_4,
            sort_enable_label=bool(sort_enable_label),
            sort_enable_qty=bool(sort_enable_qty),
            sort_enable_item_key=bool(sort_enable_item_key),
            sort_enable_location=bool(sort_enable_location),
            sort_dir_label=sort_dir_label,
            sort_dir_qty=sort_dir_qty,
            sort_dir_item_key=sort_dir_item_key,
            sort_dir_location=sort_dir_location,
        )
        cfg["admin"]["archive_retention_days"] = int(archive_retention_days)
        settings.save(cfg)
        return RedirectResponse(url="/settings?msg=Saved", status_code=303)

    @app.post("/archives/purge")
    def purge_archives(days: int = Form(14)):
        removed = batch_manager.purge_archives(days)
        return RedirectResponse(url=f"/?msg=Purged+{removed}+old+batches", status_code=303)

    @app.post("/resolve/clear")
    def resolve_clear(from_page: str = Form("unprocessed")):
        removed = batch_manager.clear_unresolved_queue()
        if from_page == "dashboard":
            return RedirectResponse(url=f"/?msg=Cleared+{removed}+unprocessed+queue+item(s)", status_code=303)
        return RedirectResponse(url=f"/unprocessed?msg=Cleared+{removed}+queue+items", status_code=303)

    @app.get("/resolve", response_class=HTMLResponse)
    def resolve_page(request: Request, msg: str = ""):
        return _templates().TemplateResponse("resolve_match.html", {"request": request, "rows": _unresolved_for_ui(), "message": msg})

    @app.get("/unprocessed", response_class=HTMLResponse)
    def unprocessed_page(request: Request, msg: str = ""):
        return _templates().TemplateResponse("resolve_match.html", {"request": request, "rows": _unresolved_for_ui(), "message": msg})

    @app.post("/resolve/remove")
    async def resolve_remove(label_pdf: str = Form(...)):
        ok = batch_manager.remove_unresolved_entry(label_pdf)
        if ok:
            return RedirectResponse(url="/unprocessed?msg=Queue+item+removed", status_code=303)
        return RedirectResponse(url="/unprocessed?msg=Queue+item+not+found", status_code=303)

    @app.post("/resolve/assign")
    async def resolve_assign(label_pdf: str = Form(...), order_id: str = Form(...)):
        result = batch_manager.resolve_unmatched(label_pdf, order_id)
        if result.get("ok"):
            return RedirectResponse(url="/unprocessed?msg=Resolved+and+output+generated", status_code=303)
        return RedirectResponse(url=f"/unprocessed?msg={result.get('error', 'Resolve+failed')}", status_code=303)

    return app


app = create_app()

