from __future__ import annotations

import copy
import csv
import json
import logging
import os
import re
import signal
import shutil
import subprocess
import tempfile
import threading
import zipfile
from difflib import SequenceMatcher
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import fitz
from pypdf import PdfReader, PdfWriter
from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .batch_manager import BatchManager
from .i18n import normalize_ui_language, translate_ui
from .label_text_extractor import extract_label_signals
from .overlay_renderer import build_overlay_lines, create_info_panel_overlay_pdf, create_overlay_pdf, get_page_size
from .pdf_merge import merge_overlays_on_first_page
from .settings_manager import SettingsManager
from .utils import sanitize_filename, setup_logging


settings = SettingsManager()
setup_logging(settings.logs_folder / "label_enricher.log")
batch_manager = BatchManager(settings)
item_db = batch_manager.item_db


def _templates() -> Jinja2Templates:
    tpl = Jinja2Templates(directory=str(settings.resource_dir / "templates"))
    ui_cfg = settings.config.get("ui", {}) or {}
    tpl.env.globals["comic_mode"] = bool(ui_cfg.get("comic_mode", False))
    tpl.env.globals["ui_font_mode"] = str(ui_cfg.get("font_mode", "default") or "default")
    lang = normalize_ui_language(str(ui_cfg.get("language_mode", "en") or "en"))
    tpl.env.globals["ui_language_mode"] = lang
    tpl.env.globals["tr"] = lambda text, **kwargs: translate_ui(text, lang=lang, **kwargs)
    return tpl

def _current_ui_lang() -> str:
    ui_cfg = settings.config.get("ui", {}) or {}
    return normalize_ui_language(str(ui_cfg.get("language_mode", "en") or "en"))


def _ui(text: str | None, **kwargs: Any) -> str:
    return translate_ui(text, lang=_current_ui_lang(), **kwargs)


def _redirect_with_message(url: str, message: str = "", status_code: int = 303) -> RedirectResponse:
    if not message:
        return RedirectResponse(url=url, status_code=status_code)
    sep = "&" if "?" in url else "?"
    return RedirectResponse(url=f"{url}{sep}msg={quote_plus(message)}", status_code=status_code)


def _redirect_ui(url: str, text: str, status_code: int = 303, **kwargs: Any) -> RedirectResponse:
    return _redirect_with_message(url, _ui(text, **kwargs), status_code=status_code)


def _join_ui_parts(parts: list[str], sep: str = " | ") -> str:
    return sep.join([str(p) for p in parts if str(p).strip()])


def _combined_pdf_browser_url(path: str, message: str = "") -> str:
    encoded_path = quote_plus(str(path or ""))
    if not encoded_path:
        return "/"
    url = f"/batch/view-pdf?path={encoded_path}"
    if message:
        url += f"&msg={quote_plus(message)}"
    return url


def _batch_counts_text(summary: dict[str, Any]) -> str:
    return _ui(
        "Matched: {matched}, unresolved: {unresolved}, errors: {errors}",
        matched=summary.get("matched", 0),
        unresolved=summary.get("unresolved", 0),
        errors=summary.get("errors", 0),
    )


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


def _find_executable(candidates: list[str]) -> str | None:
    for candidate in candidates:
        if not candidate:
            continue
        expanded = os.path.expandvars(candidate)
        if os.path.isabs(expanded) and os.path.exists(expanded):
            return expanded
        found = shutil.which(expanded)
        if found:
            return found
    return None


def _acrobat_candidates() -> list[str]:
    roots = [
        Path(r"C:\Program Files\Adobe\Acrobat DC\Acrobat\Acrobat.exe"),
        Path(r"C:\Program Files\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe"),
        Path(r"C:\Program Files (x86)\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe"),
        Path(r"C:\Program Files\Adobe\Reader 11.0\Reader\AcroRd32.exe"),
        Path(r"C:\Program Files (x86)\Adobe\Reader 11.0\Reader\AcroRd32.exe"),
    ]
    return [str(p) for p in roots]


def _default_printer_name() -> str:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_Printer | Where-Object {$_.Default -eq $true} | Select-Object -First 1 -ExpandProperty Name)",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return (result.stdout or "").strip()
    except Exception:
        return ""


def _print_file(path: Path) -> bool:
    resolved = str(path.resolve())
    acrobat = _find_executable(_acrobat_candidates())
    printer_name = _default_printer_name()
    if not acrobat or not printer_name:
        return False

    try:
        result = subprocess.run(
            [acrobat, "/n", "/s", "/o", "/h", "/t", resolved, printer_name],
            capture_output=True,
            timeout=45,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        logging.exception("Acrobat print helper failed for %s", resolved)
        return False

def _schedule_shutdown(delay_seconds: float = 0.8) -> None:
    def _shutdown() -> None:
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            os._exit(0)

    threading.Timer(delay_seconds, _shutdown).start()


def _launch_stop_script() -> bool:
    stop_bat = settings.base_dir / "STOP_LABEL_ENRICHER.bat"
    if not stop_bat.exists():
        return False
    try:
        flags = 0
        for name in ("DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP"):
            flags |= int(getattr(subprocess, name, 0) or 0)
        subprocess.Popen(
            ["cmd.exe", "/c", f'set "LABEL_ENRICHER_NO_PAUSE=1" && call "{stop_bat}"'],
            cwd=str(settings.base_dir),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=flags,
            close_fds=True,
        )
        return True
    except Exception:
        logging.exception("Failed to launch stop script")
        return False


def _unresolved() -> list[dict[str, Any]]:
    return batch_manager._load_unresolved_queue()

def _human_reason(reason: str) -> str:
    r = (reason or "").strip().lower()

    if r == "ambiguous_or_low_confidence":
        return "Could not confidently match this label to one order."
    if r == "amazon_order_not_found_in_report":
        return "Amazon label order ID was not found in the uploaded Amazon report."
    if r == "no_compatible_order_source":
        return "No compatible order source was found for this label platform in the current staged batch."
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


def _auto_added_review_count() -> int:
    return item_db.auto_added_review_count()

def _queue_counts() -> tuple[int, int]:
    unresolved_count = len(_unresolved())
    review_count = _needs_review_count()
    return unresolved_count, review_count


def _queue_guard_redirect(action: str = "opening combined pdf", *, include_review_queue: bool = True) -> RedirectResponse | None:
    unresolved_count, review_count = _queue_counts()
    if unresolved_count > 0:
        return _redirect_ui("/unprocessed", "Address {count} unprocessed label(s) before {action}.", count=unresolved_count, action=_ui(action))
    if include_review_queue and review_count > 0:
        return _redirect_ui("/items/review", "Address {count} items needing review before {action}.", count=review_count, action=_ui(action))
    return None


def _settings_changed_since_latest_batch() -> bool:
    snap = batch_manager.latest_batch_snapshot()
    batch_dir = Path(str(snap.get("batch_dir", ""))) if isinstance(snap, dict) and snap.get("batch_dir") else None
    if not batch_dir or not batch_dir.exists():
        return False
    try:
        baseline = batch_dir.stat().st_mtime + 0.5
        candidates = [settings.config_path, settings.items_csv_path]
        for path in candidates:
            if path.exists() and path.stat().st_mtime > baseline:
                return True
        return False
    except Exception:
        return False


def _safe_items_mtime_iso() -> str:
    try:
        return datetime.fromtimestamp(settings.items_csv_path.stat().st_mtime).isoformat()
    except Exception:
        return ""

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



def _norm_merge_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _row_display_name(row: dict[str, Any]) -> str:
    return str(row.get("custom_label") or row.get("item_title") or row.get("amazon_sku") or row.get("amazon_asin") or row.get("ebay_item_number") or "Untitled").strip()


def _row_identity_bits(row: dict[str, Any]) -> tuple[bool, bool]:
    has_ebay = bool(str(row.get("ebay_item_number", "") or "").strip())
    has_amz = bool(str(row.get("amazon_sku", "") or "").strip() or str(row.get("amazon_asin", "") or "").strip())
    return has_ebay, has_amz


def _find_hint_for_row(row: dict[str, Any], hints: list[dict[str, str]]) -> dict[str, str] | None:
    ebay = str(row.get("ebay_item_number", "") or "").strip()
    asin = str(row.get("amazon_asin", "") or "").strip().upper()
    for hint in hints:
        hint_ebay = str(hint.get("ebay_item_number", "") or "").strip()
        hint_asin = str(hint.get("asin", "") or "").strip().upper()
        if ebay and hint_ebay and ebay == hint_ebay:
            return hint
        if asin and hint_asin and asin == hint_asin:
            return hint
    return None


def _build_items_assist(rows: list[dict[str, Any]], hints: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, list[dict[str, Any]]]]:
    hint_matches: dict[str, dict[str, str]] = {}
    merge_candidates: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("_row_key", "") or "")
        if not key:
            continue
        hint = _find_hint_for_row(row, hints)
        if hint:
            hint_payload = {
                "label": str(hint.get("label", "") or "").strip(),
                "location": str(hint.get("location", "") or "").strip(),
                "asin": str(hint.get("asin", "") or "").strip().upper(),
                "ebay_item_number": str(hint.get("ebay_item_number", "") or "").strip(),
            }
            already_applied = True
            if hint_payload["label"] and str(row.get("custom_label", "") or "").strip() != hint_payload["label"]:
                already_applied = False
            if hint_payload["location"] and str(row.get("location", "") or "").strip() != hint_payload["location"]:
                already_applied = False
            if hint_payload["asin"] and str(row.get("amazon_asin", "") or "").strip().upper() != hint_payload["asin"]:
                already_applied = False
            if hint_payload["ebay_item_number"] and str(row.get("ebay_item_number", "") or "").strip() != hint_payload["ebay_item_number"]:
                already_applied = False
            if not already_applied:
                hint_matches[key] = hint_payload
    for row in rows:
        row_key = str(row.get("_row_key", "") or "")
        if not row_key:
            continue
        row_label = _norm_merge_text(str(row.get("custom_label", "") or ""))
        row_title = _norm_merge_text(str(row.get("item_title", "") or ""))
        row_loc = _norm_merge_text(str(row.get("location", "") or ""))
        row_var = _norm_merge_text(str(row.get("variation_options", "") or ""))
        row_has_ebay, row_has_amz = _row_identity_bits(row)
        row_hint = hint_matches.get(row_key)
        ranked: list[dict[str, Any]] = []
        for other in rows:
            other_key = str(other.get("_row_key", "") or "")
            if not other_key or other_key == row_key:
                continue
            other_has_ebay, other_has_amz = _row_identity_bits(other)
            cross_platform = (row_has_ebay and other_has_amz) or (row_has_amz and other_has_ebay) or str(row.get("platform", "")) == "both" or str(other.get("platform", "")) == "both"
            reasons: list[str] = []
            score = 0
            other_hint = hint_matches.get(other_key)
            if row_hint and other_hint and row_hint == other_hint:
                score += 300
                reasons.append("same hint import ties these platform IDs together")
            if not cross_platform and score == 0:
                continue
            other_label = _norm_merge_text(str(other.get("custom_label", "") or ""))
            other_title = _norm_merge_text(str(other.get("item_title", "") or ""))
            other_loc = _norm_merge_text(str(other.get("location", "") or ""))
            other_var = _norm_merge_text(str(other.get("variation_options", "") or ""))
            if row_label and other_label and row_label == other_label:
                score += 140
                reasons.append("same custom label")
            if row_title and other_title:
                if row_title == other_title:
                    score += 100
                    reasons.append("same title")
                else:
                    ratio = SequenceMatcher(None, row_title, other_title).ratio()
                    if ratio >= 0.92:
                        score += 70
                        reasons.append(f"very similar title ({ratio:.0%})")
            if row_loc and other_loc and row_loc == other_loc:
                score += 20
                reasons.append("same location")
            if row_var and other_var and row_var == other_var:
                score += 25
                reasons.append("same variation options")
            if score < 120:
                continue
            ranked.append({
                "row_key": other_key,
                "score": score,
                "label": _row_display_name(other),
                "preview": _join_ui_parts([_row_display_name(other), ", ".join(reasons[:2]) if reasons else "possible duplicate"], sep=" | "),
            })
        ranked.sort(key=lambda x: (-int(x.get("score", 0)), str(x.get("label", "")).lower()))
        merge_candidates[row_key] = ranked[:5]
    return hint_matches, merge_candidates

def _label_hints_path() -> Path:
    return settings.base_dir / "label_location_hints.csv"

def _norm_hint_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _normalize_hint_asin(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    upper = raw.upper()
    direct = re.search(r"\b(B[0-9A-Z]{9})\b", upper)
    if direct:
        return direct.group(1)
    for pattern in [r"/dp/([A-Z0-9]{10})", r"/gp/product/([A-Z0-9]{10})", r"[?&]asin=([A-Z0-9]{10})"]:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            candidate = match.group(1).upper()
            if re.fullmatch(r"B[0-9A-Z]{9}", candidate):
                return candidate
    return ""


def _normalize_hint_ebay_item_number(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    direct = re.search(r"\b(\d{10,14})\b", raw)
    if direct:
        return direct.group(1)
    for pattern in [r"/itm/(?:[^/]+/)?(\d{10,14})", r"item=(\d{10,14})", r"\bitemid\b[^0-9]*(\d{10,14})"]:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            return match.group(1)
    return ""
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
            col_amazon_link = pick("amazon link", "amz link")
            col_ebay_item = pick("ebay item #", "ebay item", "ebay item number", "item #")
            col_ebay_link = pick("ebay link")
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
                asin_raw = (row.get(col_asin, "") if col_asin else "").strip()
                amazon_link_raw = (row.get(col_amazon_link, "") if col_amazon_link else "").strip()
                ebay_item_raw = (row.get(col_ebay_item, "") if col_ebay_item else "").strip()
                ebay_link_raw = (row.get(col_ebay_link, "") if col_ebay_link else "").strip()
                asin = _normalize_hint_asin(asin_raw or amazon_link_raw)
                ebay_item_number = _normalize_hint_ebay_item_number(ebay_item_raw or ebay_link_raw)
                key = label.lower()
                prev = out_by_label.get(key, {"label": label, "location": "", "asin": "", "ebay_item_number": ""})
                if location and not prev.get("location"):
                    prev["location"] = location
                if asin and not prev.get("asin"):
                    prev["asin"] = asin
                if ebay_item_number and not prev.get("ebay_item_number"):
                    prev["ebay_item_number"] = ebay_item_number
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
            "asin": _normalize_hint_asin(str(r.get("asin", "") or "").strip()),
            "ebay_item_number": _normalize_hint_ebay_item_number(str(r.get("ebay_item_number", "") or "").strip()),
        })
    p = _label_hints_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["internal_label", "location", "asin", "ebay_item_number"])
        writer.writeheader()
        for row in clean:
            writer.writerow({
                "internal_label": row["label"],
                "location": row["location"],
                "asin": row["asin"],
                "ebay_item_number": row["ebay_item_number"],
            })
    tmp.replace(p)
    return len(clean)
def _items_sync_stage_path() -> Path:
    settings.logs_folder.mkdir(parents=True, exist_ok=True)
    return settings.logs_folder / "items_sync_stage.json"

def _load_items_sync_stage() -> dict[str, Any] | None:
    p = _items_sync_stage_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None

def _save_items_sync_stage(data: dict[str, Any]) -> None:
    p = _items_sync_stage_path()
    p.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

def _clear_items_sync_stage() -> None:
    p = _items_sync_stage_path()
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass


def _manual_entry_draft_path() -> Path:
    settings.logs_folder.mkdir(parents=True, exist_ok=True)
    return settings.logs_folder / "manual_entry_draft.json"


def _load_manual_entry_draft() -> dict[str, Any] | None:
    p = _manual_entry_draft_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


def _save_manual_entry_draft(data: dict[str, Any]) -> None:
    p = _manual_entry_draft_path()
    p.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


def _clear_manual_entry_draft() -> None:
    p = _manual_entry_draft_path()
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass

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


def _manual_incoming_folder() -> Path:
    return settings.manual_incoming_folder


def _manual_split_runtime_dir() -> Path:
    path = _manual_incoming_folder() / "_split_pages"
    path.mkdir(parents=True, exist_ok=True)
    return path

def _manual_sort_key(path: Path) -> list[Any]:
    parts = re.split(r"(\d+)", str(path).replace("\\", "/"))
    out: list[Any] = []
    for part in parts:
        if not part:
            continue
        out.append(int(part) if part.isdigit() else part.lower())
    return out


def _expand_manual_label_pdf(src: Path) -> list[Path]:
    if src.suffix.lower() != ".pdf" or "packing slip" in src.name.lower():
        return []
    try:
        reader = PdfReader(str(src))
    except Exception:
        logging.exception("Failed to open manual PDF while checking page count: %s", src)
        return [src]

    if len(reader.pages) <= 1:
        return [src]

    split_dir = _manual_split_runtime_dir()
    base = sanitize_filename(src.stem) or "label"
    out: list[Path] = []
    for i, page in enumerate(reader.pages, start=1):
        one = split_dir / f"{base}__p{i:03d}.pdf"
        try:
            writer = PdfWriter()
            writer.add_page(page)
            with one.open("wb") as f:
                writer.write(f)
            out.append(one)
        except Exception:
            logging.exception("Failed to split manual PDF page %s from %s", i, src)
    return out or [src]


def _manual_label_pdf_paths() -> list[Path]:
    manual_root = _manual_incoming_folder()
    _extract_zip_files_into(manual_root)
    split_dir = _manual_split_runtime_dir()
    shutil.rmtree(split_dir, ignore_errors=True)
    split_dir.mkdir(parents=True, exist_ok=True)

    expanded: list[Path] = []
    seen: set[str] = set()
    for p in sorted(manual_root.rglob("*.pdf"), key=_manual_sort_key):
        if "packing slip" in p.name.lower() or "_split_pages" in p.parts:
            continue
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            continue
        seen.add(key)
        expanded.extend(_expand_manual_label_pdf(p))
    expanded.sort(key=_manual_sort_key)
    return expanded

def _resolve_manual_label_paths(label_pdf: str) -> list[Path]:
    manual_root = _manual_incoming_folder().resolve()
    selected = Path(label_pdf)
    candidates: list[Path] = []
    if label_pdf:
        candidates.append(selected)
        candidates.append(manual_root / selected.name)

    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        if resolved.exists() and str(resolved).lower().startswith(str(manual_root).lower()):
            if "_split_pages" in resolved.parts:
                return [resolved]
            return _expand_manual_label_pdf(resolved)

    for p in _manual_label_pdf_paths():
        if p.name == selected.name:
            return [p]
    return []


def _extract_zip_files_into(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    unzip_root = root / "_unzipped"
    unzip_root.mkdir(parents=True, exist_ok=True)
    for zip_path in sorted(root.glob("*.zip"), key=_manual_sort_key):
        dest_root = unzip_root / (sanitize_filename(zip_path.stem) or zip_path.stem or "zip")
        shutil.rmtree(dest_root, ignore_errors=True)
        dest_root.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                for member in zf.infolist():
                    if member.is_dir():
                        continue
                    target_name = Path(member.filename).name
                    if not target_name:
                        continue
                    dest = dest_root / target_name
                    with zf.open(member) as src, dest.open("wb") as out:
                        shutil.copyfileobj(src, out)
        except zipfile.BadZipFile:
            continue

def _available_label_pdfs(extract_zip: bool = False) -> list[Path]:
    if extract_zip:
        batch_manager._extract_zip_files()
    files = [Path(p) for p in batch_manager.scan_inputs().get("files", []) if str(p).lower().endswith(".pdf")]
    labels = [p for p in files if "packing slip" not in p.name.lower()]
    if not labels:
        latest = batch_manager.latest_batch_snapshot()
        batch_dir_raw = str((latest or {}).get("batch_dir", "") or "").strip()
        batch_dir = Path(batch_dir_raw) if batch_dir_raw else None
        if batch_dir and batch_dir.exists():
            archive_dir = batch_dir / "input_archive"
            archived = [p for p in archive_dir.glob("*.pdf") if "packing slip" not in p.name.lower()]
            if archived:
                labels = archived
            else:
                output_dir = batch_dir / "output_pdfs"
                labels = [p for p in output_dir.glob("*.pdf") if p.name.lower() != "combined_print.pdf"]
    labels = [p for p in labels if p.exists()]
    labels.sort()
    return labels

def _manual_label_options() -> list[dict[str, str]]:
    manual_root = _manual_incoming_folder()
    values: set[str] = {str(p) for p in _manual_label_pdf_paths()}

    unresolved_by_path: dict[str, str] = {}
    manual_root_str = str(manual_root.resolve()).lower()
    for row in _unresolved():
        p = str(row.get("label_pdf", "") or "").strip()
        if not p:
            continue
        try:
            resolved = str(Path(p).resolve()).lower()
        except Exception:
            resolved = p.lower()
        if not resolved.startswith(manual_root_str):
            continue
        values.add(p)
        ident = str(row.get("label_identity", "") or "").strip()
        if ident:
            unresolved_by_path[p] = ident

    out: list[dict[str, str]] = []
    for s in sorted(values, key=lambda value: _manual_sort_key(Path(value))):
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
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def _grab(pattern: str, flags: int = re.IGNORECASE) -> str:
        m = re.search(pattern, text, flags)
        return (m.group(1).strip() if m else "")

    def _next_meaningful_after(patterns: list[str]) -> str:
        for pat in patterns:
            for i, ln in enumerate(lines):
                if re.search(pat, ln, re.IGNORECASE):
                    for cand in lines[i + 1 : i + 6]:
                        cc = cand.strip(" :,-")
                        cl = cc.lower()
                        if not cc:
                            continue
                        if cl in {"phone", "item", "order", "shipping", "tracking"}:
                            continue
                        if cl.startswith("opens in a new window"):
                            continue
                        if re.fullmatch(r"\(\d+\)", cc):
                            continue
                        return cc
        return ""

    def _looks_like_title_line(ln: str) -> bool:
        raw = str(ln or "").strip()
        ll = raw.lower().strip(":")
        if len(ll) < 8:
            return False
        bad_parts = [
            "order total includes ebay collected tax",
            "we collect and remit tax",
            "learn moreopens",
            "about ebay",
            "copyright",
            "seller hub",
            "skip to main content",
            "view more detailsopens",
            "tell us what you thinkopens",
            "accessibility, user agreement",
        ]
        if any(bp in ll for bp in bad_parts):
            return False
        if re.search(r"\b(?:asin|sku|order item id|condition|tracking|order id|zip|qty|quantity|buyer paid|sold|subtotal|shipping|sales tax|item total|grand total|order total|funds status|payment|ship to|buyer name|seller name|shipping service|sales record no)\b", ll):
            return False
        if ll in {"order details", "item", "shipping", "payment", "status", "product name", "order summary", "what your buyer paid"}:
            return False
        if re.fullmatch(r"\$?\d+(?:\.\d{2})?", ll):
            return False
        return True

    def _find_title() -> str:
        ebay_top = _grab(r"(?is)order details\s+(.+?)\s+shipped")
        if _looks_like_title_line(ebay_top):
            return ebay_top
        marker_patterns = [r"custom label \(sku\)", r"item id\s*:", r"sku\s*:", r"asin\s*:", r"condition\s*:", r"order item id\s*:", r"tracking", r"quantity"]
        for i, ln in enumerate(lines):
            if any(re.search(pat, ln, re.IGNORECASE) for pat in marker_patterns):
                for cand in reversed(lines[max(0, i - 4):i]):
                    if _looks_like_title_line(cand):
                        return cand
        for i, ln in enumerate(lines):
            if re.fullmatch(r"item", ln, re.IGNORECASE):
                section: list[str] = []
                for cand in lines[i + 1 : i + 8]:
                    if re.search(r"(?:custom label \(sku\)|item id\s*:|sku\s*:|asin\s*:|tracking|quantity)", cand, re.IGNORECASE):
                        break
                    if _looks_like_title_line(cand):
                        section.append(cand)
                if section:
                    return max(section, key=len)
        for i, ln in enumerate(lines):
            if re.search(r"quantity\s+product details", ln, re.IGNORECASE):
                section: list[str] = []
                for cand in lines[i + 1 : i + 10]:
                    if re.search(r"(?:sku\s*:|asin\s*:|condition\s*:|order item id\s*:|item subtotal|grand total|item total)", cand, re.IGNORECASE):
                        break
                    if _looks_like_title_line(cand):
                        section.append(cand)
                if section:
                    return max(section, key=len)
        candidates = [ln for ln in lines if _looks_like_title_line(ln)]
        return max(candidates, key=len).strip() if candidates else ""

    order_id = _grab(r"\b(\d{3}-\d{7}-\d{7})\b")
    asin = _grab(r"\b(B[0-9A-Z]{9})\b")
    sku = _grab(r"\bsku\b\s*[:#-]?\s*([A-Z0-9._-]{2,64})")
    if not sku:
        m = re.search(r"\bsku\b\s*[:#-]?\s*\n\s*([A-Z0-9._-]{2,64})", text, re.IGNORECASE)
        sku = (m.group(1).strip() if m else "")
    ebay_item = _grab(r"\bitem id\b\s*[:#-]?\s*(\d{10,14})")
    if not ebay_item:
        ebay_item = _grab(r"\b(\d{10,14})\b")
    tracking = _grab(r"\btracking\b\s*[:#-]?\s*(1Z[0-9A-Z]{16}|9[0-9]{19,24}|\d{15}|\d{20}|\d{22})")
    if not tracking:
        tracking = _grab(r"\b(1Z[0-9A-Z]{16}|9[0-9]{19,24})\b")
    qty = _grab(r"(?is)\b(?:qty|quantity)\b[^\d]{0,20}(\d{1,4})")
    if not qty:
        qty = _grab(r"(?m)^\s*(\d{1,3})\s+\$?\d")
    if not qty:
        qty = "1"
    total = _grab(r"(?im)^\s*(?:grand total|order total|item total)\*{0,2}\s*[:$]*\s*\$?\s*([0-9]+(?:\.\d{2})?)")
    if not total:
        total = _grab(r"\b(?:total|paid|amount)\b[^\d$]{0,12}\$?\s*([0-9]+(?:\.\d{2})?)")

    recipient_name = _next_meaningful_after([r"^buyer name:?$", r"^buyer$", r"^ship to$", r"^shipping address:?$"])
    if not recipient_name:
        recipient_name = _grab(r"(?ims)^buyer\s*[\r\n]+\s*([A-Za-z][^\n]{1,80})$")
    if not recipient_name:
        recipient_name = _grab(r"(?im)^buyer\s+([A-Za-z][^\n]{1,80})$")
    if recipient_name and (recipient_name.strip().isdigit() or recipient_name.strip() == ebay_item):
        recipient_name = ""

    title = _find_title()
    platform = "ebay" if (("ebay" in lower or ebay_item) and not sku) else ("amazon" if ("amazon" in lower or asin or sku) else "amazon")
    item_key = sku or ebay_item or asin
    label_ref = tracking or recipient_name or ""
    if platform == "ebay" and label_ref and label_ref == item_key:
        label_ref = recipient_name or ""

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
                "manual_prefix": "",
                "messy_text": "",
            }


def _split_manual_text_chunks(blob: str) -> list[str]:
    text = (blob or "").strip()
    if not text:
        return []

    def _split_on_positions(matches: list[re.Match[str]]) -> list[str]:
        chunks: list[str] = []
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if (i + 1) < len(matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
        return chunks

    skip_matches = list(re.finditer(r"(?im)^skip to main content\s*$", text))
    if len(skip_matches) >= 2:
        chunks = _split_on_positions(skip_matches)
        if chunks:
            return chunks

    detail_matches = list(re.finditer(r"(?im)^order details\s*$", text))
    if len(detail_matches) >= 2:
        chunks = _split_on_positions(detail_matches)
        if chunks:
            return chunks

    ship_matches = list(re.finditer(r"(?im)^shipping address:\s*$", text))
    if len(ship_matches) >= 2:
        chunks = _split_on_positions(ship_matches)
        if chunks:
            return chunks

    order_matches = list(re.finditer(r"\b\d{3}-\d{7}-\d{7}\b", text))
    if len(order_matches) >= 2:
        chunks = _split_on_positions(order_matches)
        if chunks:
            return chunks

    ebay_order_matches = list(re.finditer(r"\b\d{2}-\d{5}-\d{5}\b", text))
    if len(ebay_order_matches) >= 2:
        chunks = _split_on_positions(ebay_order_matches)
        if chunks:
            return chunks
    parts = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
    return parts if parts else [text]


def _manual_batch_defaults(label_options: list[dict[str, str]], limit: int | None = None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if limit is None:
        visible = label_options[:]
    else:
        visible = label_options[: max(1, min(limit, len(label_options)))]
    for idx, opt in enumerate(visible, start=1):
        out.append(
            {
                "group_id": f"manual-group-{idx:03d}",
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
                "manual_prefix": "",
                "messy_text": "",
            }
        )
    if not out:
        out.append(
            {
                "group_id": "manual-group-001",
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
                "manual_prefix": "",
                "messy_text": "",
            }
        )
    return _decorate_manual_batch_entries(out)


def _decorate_manual_batch_entries(entries: list[dict[str, str]]) -> list[dict[str, str]]:
    groups: dict[str, list[dict[str, str]]] = {}
    out: list[dict[str, str]] = []
    next_group = 1
    for entry in entries:
        row = dict(entry)
        group_id = str(row.get("group_id", "") or "").strip()
        if not group_id:
            group_id = f"manual-group-{next_group:03d}"
            next_group += 1
        row["group_id"] = group_id
        groups.setdefault(group_id, []).append(row)
        out.append(row)

    group_order_map = {group_id: idx for idx, group_id in enumerate(groups.keys(), start=1)}
    for group_id, rows in groups.items():
        item_count = len(rows)
        for idx, row in enumerate(rows, start=1):
            row["group_order"] = str(group_order_map[group_id])
            row["group_item_index"] = str(idx)
            row["group_item_count"] = str(item_count)
    return out


def _extract_manual_items_from_text(blob: str) -> list[dict[str, Any]]:
    text = str(blob or "").strip()
    if not text:
        return []

    marker_re = re.compile(
        r"(?im)(?:\bitem\s*(?:id|#)\b\s*[:#-]?\s*(\d{10,14})|\bsku\b\s*[:#-]?\s*([A-Z0-9._-]{2,64})|\basin\b\s*[:#-]?\s*(B[0-9A-Z]{9}))"
    )
    matches = list(marker_re.finditer(text))
    chunks: list[str] = []
    if len(matches) >= 2:
        for idx, match in enumerate(matches):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
    else:
        chunks = [text]

    items: list[dict[str, Any]] = []
    for chunk in chunks:
        parsed = _apply_manual_db_prefill(_extract_manual_prefill_from_text(chunk))
        marker = marker_re.search(chunk)
        if marker:
            item_id = str(marker.group(1) or "").strip()
            sku = str(marker.group(2) or "").strip()
            asin = str(marker.group(3) or "").strip().upper()
            if asin and not str(parsed.get("item_asin", "") or "").strip():
                parsed["item_asin"] = asin
            if item_id and not str(parsed.get("item_key", "") or "").strip():
                parsed["item_key"] = item_id
            if sku and not str(parsed.get("item_key", "") or "").strip():
                parsed["item_key"] = sku
            if asin and not str(parsed.get("item_key", "") or "").strip() and str(parsed.get("platform", "") or "").strip().lower() == "amazon":
                parsed["item_key"] = asin
        parsed["messy_text"] = chunk
        items.append(parsed)
    return items


def _expand_manual_row_entries(row: dict[str, str], parse_text: bool = True) -> list[dict[str, str]]:
    base = dict(row)
    raw_text = str(base.get("messy_text", "") or "")
    if not parse_text or not raw_text.strip():
        return [base]

    parsed_items = _extract_manual_items_from_text(raw_text)
    if not parsed_items:
        return [base]

    out: list[dict[str, str]] = []
    for parsed in parsed_items:
        entry = dict(base)
        for key in ["platform", "item_key", "label_ref", "item_asin", "title", "quantity", "total_paid", "location", "custom_label"]:
            val = str(parsed.get(key, "") or "").strip()
            if val:
                entry[key] = val
        entry["messy_text"] = str(parsed.get("messy_text", raw_text) or raw_text)
        if not str(entry.get("use_title_as_label", "") or "").strip():
            entry["use_title_as_label"] = "1"
        out.append(entry)
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
                "group_id": str(form.get(prefix + "group_id", "") or "").strip(),
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
                "manual_prefix": _manual_prefix_from_form(form, prefix),
                "messy_text": str(form.get(prefix + "messy_text", "") or ""),
            }
        )
    return _decorate_manual_batch_entries(rows)

def _manual_prefix_from_form(form: dict[str, Any], prefix: str) -> str:
    options = [
        ("overnight", "Overnight"),
        ("two_day", "Two-Day"),
        ("standard", "Standard"),
        ("sd", "SD"),
        ("replacement", "REPLACEMENT"),
    ]
    for field, value in options:
        if form.get(prefix + f"shipping_{field}"):
            return value
    return ""


def _manual_label_ref_from_pdf(src: Path) -> str:
    try:
        signals = extract_label_signals(src)
    except Exception:
        return ""
    tracking = str(signals.get("tracking_number", "") or "").strip()
    if tracking:
        return tracking
    return str(signals.get("recipient_name", "") or "").strip()


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


def _manual_platform_for_label(label_pdf: str | Path, fallback: str = "amazon") -> str:
    raw = str(label_pdf or "").strip()
    detected = "unknown"
    if raw:
        try:
            detected = detect_platform_from_path(Path(raw))
        except Exception:
            detected = "unknown"
    if detected in ("amazon", "ebay"):
        return detected
    p = str(fallback or "amazon").strip().lower()
    if p not in ("amazon", "ebay"):
        p = "amazon"
    return p


def _manual_lookup_preview_rows(query: str, platform: str, limit: int = 8) -> list[dict[str, str]]:
    q = str(query or "").strip().lower()
    if not q:
        return []
    p = str(platform or "amazon").strip().lower()
    if p not in ("amazon", "ebay"):
        p = "amazon"

    matches: list[tuple[int, dict[str, str]]] = []
    for row in item_db.load_rows():
        candidates = [
            str(row.get("item_id", "") or "").strip(),
            str(row.get("ebay_item_number", "") or "").strip(),
            str(row.get("amazon_sku", "") or "").strip(),
            str(row.get("amazon_asin", "") or "").strip().upper(),
        ]
        haystack = [c for c in candidates if c]
        if not haystack:
            continue
        score = None
        for candidate in haystack:
            candidate_l = candidate.lower()
            if candidate_l == q:
                score = 0
                break
            if candidate_l.startswith(q):
                score = 1 if score is None else min(score, 1)
            elif q in candidate_l:
                score = 2 if score is None else min(score, 2)
        if score is None:
            continue
        matches.append((score, row))

    matches.sort(key=lambda item: (item[0], str(item[1].get("item_id", "") or "")))
    out: list[dict[str, str]] = []
    for _, row in matches[: max(1, limit)]:
        out.append({
            "item_id": str(row.get("item_id", "") or "").strip(),
            "ebay_item_number": str(row.get("ebay_item_number", "") or "").strip(),
            "amazon_sku": str(row.get("amazon_sku", "") or "").strip(),
            "amazon_asin": str(row.get("amazon_asin", "") or "").strip().upper(),
            "custom_label": str(row.get("custom_label", "") or "").strip(),
            "location": str(row.get("location", "") or "").strip(),
            "item_title": str(row.get("item_title", "") or "").strip(),
        })
    return out

def _apply_manual_db_prefill(prefill: dict[str, Any]) -> dict[str, Any]:
    data = dict(prefill or {})
    platform = str(data.get("platform", "amazon") or "amazon").strip().lower()
    key = str(data.get("item_key", "") or "").strip()
    asin = str(data.get("item_asin", "") or "").strip().upper()
    if platform == "amazon" and not asin and key.upper().startswith("B") and len(key) == 10:
        asin = key.upper()
        data["item_asin"] = asin

    row = _manual_lookup_row(item_db.index(), platform, key, asin) if (key or asin) else None
    if row is None:
        return data

    db_label = str(row.get("custom_label", "") or "").strip()
    db_title = str(row.get("item_title", "") or "").strip()
    db_location = str(row.get("location", "") or "").strip()
    db_sku = str(row.get("amazon_sku", "") or "").strip()
    db_asin = str(row.get("amazon_asin", "") or "").strip().upper()
    db_ebay = str(row.get("ebay_item_number", "") or "").strip()

    if db_label:
        data["custom_label"] = db_label
    if db_title and not str(data.get("title", "") or "").strip():
        data["title"] = db_title
    if db_location:
        data["location"] = db_location
    if platform == "amazon":
        if db_sku and not key:
            data["item_key"] = db_sku
        if db_asin and not asin:
            data["item_asin"] = db_asin
    if platform == "ebay" and db_ebay and not key:
        data["item_key"] = db_ebay
    return data

TRACKING_REF_RE = re.compile(r"\b(1Z[0-9A-Z]{16}|9[0-9]{19,24}|\d{15}|\d{20}|\d{22})\b", re.IGNORECASE)

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
    if lg == "qty,label,location":
        return "qty_label_loc_inline"
    if lg == "qty,label;total,location":
        return "qty_label_then_total_loc"
    if lg == "label,qty,total":
        return "label_qty_total_inline"
    if lg == "location,label,qty,total":
        return "location_label_qty_total_inline"
    if fo == "label,qty,total,location,title" and inf == "label,qty,total":
        return "label_qty_total_inline"
    if fo == "location,label,qty,total,title" and inf == "location,label,qty,total":
        return "location_label_qty_total_inline"
    if inf == "":
        return "stacked"
    return "custom"



def _line_groups_for_mode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m == "qty_label_loc_inline":
        return "qty,label,location"
    if m == "qty_label_then_total_loc":
        return "qty,label;total,location"
    if m == "label_qty_total_inline":
        return "label,qty,total"
    if m == "location_label_qty_total_inline":
        return "location,label,qty,total"
    return ""



def _apply_line_layout_mode(line_layout_mode: str, field_order_csv: str, inline_fields_csv: str) -> tuple[str, str]:
    mode = (line_layout_mode or "custom").strip().lower()
    if mode == "stacked":
        return "label,qty,total,location,title", ""
    if mode == "qty_label_loc_inline":
        return "qty,label,location,total,title", ""
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

    output_sort = settings.config.get("output_sort", {})
    ui_cfg = settings.config.get("ui", {}) if isinstance(settings.config.get("ui", {}), dict) else {}
    enabled_fields = output_sort.get("enabled_fields", {}) if isinstance(output_sort.get("enabled_fields", {}), dict) else {}
    directions = output_sort.get("directions", {}) if isinstance(output_sort.get("directions", {}), dict) else {}
    priorities = output_sort.get("priority_fields", ["label", "location", "qty", "item_key"])
    priorities = priorities if isinstance(priorities, list) else ["label", "location", "qty", "item_key"]

    return {
        "margin_direction": margin_direction,
        "margin_mode": "both" if str(layout.get("overflow_mode", "backside")) == "secondary_margin" else "single",
        "font_size": int(layout.get("font_size", 16)),
        "backside_font_size": int(layout.get("backside_font_size", layout.get("font_size", 20))),
        "line_spacing": int(layout.get("line_spacing", 20)),
        "backside_line_spacing": int(layout.get("backside_line_spacing", layout.get("line_spacing", 24))),
        "strip_thickness": strip_thickness,
        "edge_padding": int(layout.get("edge_inset_y", 8)),
        "side_padding": int(layout.get("edge_inset_x", 8)),
        "spill_edge_padding": int(layout.get("spill_edge_inset_y", layout.get("edge_inset_y", 8))),
        "spill_side_padding": int(layout.get("spill_edge_inset_x", layout.get("edge_inset_x", 8))),
        "spill_strip_thickness": int(layout.get("spill_margin_box_height", layout.get("margin_box_height", 36))),
        "spill_font_size": int(layout.get("spill_font_size", 11)),
        "spill_line_spacing": int(layout.get("spill_line_spacing", 14)),
        "compact_threshold": int(layout.get("compact_threshold", 4)),
        "summary_page_min_items": int(layout.get("summary_page_min_items", layout.get("compact_threshold", 4))),
        "summary_page_mode": str(layout.get("summary_page_mode", "half_page")),
        "summary_page_orientation": str(layout.get("summary_page_orientation", "normal")),
        "summary_page_font_size": int(layout.get("summary_page_font_size", layout.get("backside_font_size", layout.get("font_size", 20)))),
        "summary_page_line_spacing": int(layout.get("summary_page_line_spacing", layout.get("backside_line_spacing", layout.get("line_spacing", 24)))),
        "summary_page_wrap_mode": str(layout.get("summary_page_wrap_mode", layout.get("wrap_mode", "word"))),
        "summary_page_text_align": str(layout.get("summary_page_text_align", layout.get("text_align", "left"))),
        "summary_page_margin": int(layout.get("summary_page_margin", 24)),
        "total_display_mode": str(layout.get("total_display_mode", "grand_total")),
        "text_align": str(layout.get("text_align", "left")),
        "wrap_mode": str(layout.get("wrap_mode", "word")),
        "line_layout_mode": _detect_line_layout_mode(field_order_csv, inline_fields_csv, line_groups_csv),
        "field_order_csv": field_order_csv,
        "inline_fields_csv": inline_fields_csv,
        "line_groups_csv": line_groups_csv,
        "inline_separator": str(layout.get("inline_separator", " | ")),
        "show_field_labels": bool(layout.get("show_field_labels", True)),
        "page_mode": str(layout.get("page_mode", "half_sheet_top")),
        "render_mode": str(layout.get("overlay_mode", "margin")),
        "archive_retention_days": int(settings.config.get("admin", {}).get("archive_retention_days", 14)),
        "ui_language_mode": str(ui_cfg.get("language_mode", "en") or "en"),
        "ui_font_mode": str(ui_cfg.get("font_mode", "default") or "default"),
        "output_sort_mode": str(output_sort.get("mode", "processed")),
        "sort_priority_1": str((priorities + ["", "", "", ""])[0]),
        "sort_priority_2": str((priorities + ["", "", "", ""])[1]),
        "sort_priority_3": str((priorities + ["", "", "", ""])[2]),
        "sort_priority_4": str((priorities + ["", "", "", ""])[3]),
        "sort_enable_label": bool(enabled_fields.get("label", True)),
        "sort_enable_qty": bool(enabled_fields.get("qty", False)),
        "sort_enable_item_key": bool(enabled_fields.get("item_key", False)),
        "sort_enable_location": bool(enabled_fields.get("location", False)),
        "sort_enable_carrier": bool(enabled_fields.get("carrier", False)),
        "sort_dir_label": str(directions.get("label", "asc")),
        "sort_dir_qty": str(directions.get("qty", "asc")),
        "sort_dir_item_key": str(directions.get("item_key", "asc")),
        "sort_dir_location": str(directions.get("location", "asc")),
        "sort_dir_carrier": str(directions.get("carrier", "asc")),
    }


def _build_preview_config(
    margin_direction: str,
    margin_mode: str,
    font_size: int,
    backside_font_size: int,
    line_spacing: int,
    backside_line_spacing: int,
    strip_thickness: int,
    edge_padding: int,
    side_padding: int,
    spill_edge_padding: int,
    spill_side_padding: int,
    spill_strip_thickness: int,
    spill_font_size: int,
    spill_line_spacing: int,
    compact_threshold: int,
    text_align: str,
    wrap_mode: str,
    line_layout_mode: str,
    field_order_csv: str,
    inline_fields_csv: str,
    line_groups_csv: str,
    inline_separator: str,
    show_field_labels: bool,
    page_mode: str,
    render_mode: str = "margin",
    output_sort_mode: str = "processed",
    sort_priority_1: str = "label",
    sort_priority_2: str = "location",
    sort_priority_3: str = "qty",
    sort_priority_4: str = "item_key",
    sort_enable_label: bool = True,
    sort_enable_qty: bool = False,
    sort_enable_item_key: bool = False,
    sort_enable_location: bool = False,
    sort_enable_carrier: bool = False,
    sort_dir_label: str = "asc",
    sort_dir_qty: str = "asc",
    sort_dir_item_key: str = "asc",
    sort_dir_location: str = "asc",
    sort_dir_carrier: str = "asc",
    summary_page_min_items: int = 4,
    summary_page_mode: str = "half_page",
    summary_page_orientation: str = "normal",
    summary_page_font_size: int = 20,
    summary_page_line_spacing: int = 24,
    summary_page_wrap_mode: str = "word",
    summary_page_text_align: str = "left",
    summary_page_margin: int = 24,
    total_display_mode: str = "grand_total",
) -> dict[str, Any]:
    cfg = copy.deepcopy(settings.config)
    layout = cfg.setdefault("print_layout", {})

    layout["font_size"] = int(font_size)
    layout["backside_font_size"] = int(backside_font_size)
    layout["line_spacing"] = int(line_spacing)
    layout["backside_line_spacing"] = int(backside_line_spacing)
    layout["edge_inset_x"] = int(side_padding)
    layout["edge_inset_y"] = int(edge_padding)
    layout["spill_edge_inset_x"] = int(spill_side_padding)
    layout["spill_edge_inset_y"] = int(spill_edge_padding)
    layout["spill_margin_box_height"] = int(spill_strip_thickness)
    layout["spill_font_size"] = int(spill_font_size)
    layout["spill_line_spacing"] = int(spill_line_spacing)
    layout["compact_threshold"] = int(compact_threshold)
    layout["summary_page_min_items"] = int(summary_page_min_items)
    layout["summary_page_mode"] = str(summary_page_mode)
    layout["summary_page_orientation"] = str(summary_page_orientation)
    layout["summary_page_font_size"] = int(summary_page_font_size)
    layout["summary_page_line_spacing"] = int(summary_page_line_spacing)
    layout["summary_page_wrap_mode"] = str(summary_page_wrap_mode)
    layout["summary_page_text_align"] = str(summary_page_text_align)
    layout["summary_page_margin"] = int(summary_page_margin)
    layout["total_display_mode"] = "subtotal" if str(total_display_mode or "").strip().lower() == "subtotal" else "grand_total"
    layout["text_align"] = str(text_align)
    layout["wrap_mode"] = str(wrap_mode)
    layout["inline_separator"] = str(inline_separator)
    layout["show_field_labels"] = bool(show_field_labels)
    layout["page_mode"] = page_mode
    mode = str(render_mode or "margin").strip().lower()
    layout["overlay_mode"] = mode if mode in {"margin", "backside", "both"} else "margin"

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
    effective_line_groups = (line_groups_csv or "").strip() if (line_layout_mode or "").strip().lower() == "custom" else _line_groups_for_mode(line_layout_mode)
    layout["field_order"] = [x.strip() for x in (field_order_csv or "").split(",") if x.strip()]
    layout["inline_fields_csv"] = inline_fields_csv
    layout["line_groups_csv"] = effective_line_groups
    layout["overflow_mode"] = "secondary_margin" if margin_mode == "both" else "backside"

    allowed = {"label", "qty", "item_key", "location", "carrier"}
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
            "carrier": bool(sort_enable_carrier),
        },
        "directions": {
            "label": "desc" if str(sort_dir_label).lower() == "desc" else "asc",
            "qty": "desc" if str(sort_dir_qty).lower() == "desc" else "asc",
            "item_key": "desc" if str(sort_dir_item_key).lower() == "desc" else "asc",
            "location": "desc" if str(sort_dir_location).lower() == "desc" else "asc",
            "carrier": "desc" if str(sort_dir_carrier).lower() == "desc" else "asc",
        },
    }
    return cfg
def _sample_lines_for_order(field_order_csv: str, inline_fields_csv: str, show_field_labels: bool, inline_separator: str, line_groups_csv: str = "") -> list[str]:
    cfg = copy.deepcopy(settings.config)
    layout = cfg.setdefault("print_layout", {})
    field_order_csv, inline_fields_csv = _apply_line_layout_mode("custom", field_order_csv, inline_fields_csv)
    effective_line_groups = (line_groups_csv or "").strip()
    layout["field_order"] = [x.strip() for x in (field_order_csv or "").split(",") if x.strip()]
    layout["inline_fields_csv"] = inline_fields_csv
    layout["line_groups_csv"] = effective_line_groups
    layout["inline_separator"] = inline_separator or " | "
    layout["show_field_labels"] = bool(show_field_labels)

    sample_order = {
        "platform": "amazon",
        "order_id": "SAMPLE-001",
        "total_paid": 26.98,
        "subtotal_paid": 24.49,
        "items": [
            {"item_id": "K3-PBV6-ON0I", "title": "64oz bowl lid for Ninja blender replacement with extra long descriptive title", "quantity": 2, "item_subtotal": 0.0},
            {"item_id": "B081YY3ZBV", "title": "Shark motor head attachment with extra long descriptive title for overflow preview", "quantity": 1, "item_subtotal": 14.50},
            {"item_id": "HV320-BODY", "title": "Body assembly for Shark handheld vacuum with longer overflow sample text", "quantity": 1, "item_subtotal": 5.00},
        ],
    }
    sample_rows = [
        {"item_id": "K3-PBV6-ON0I", "custom_label": "Ninja Bowl Lid", "location": "rack A-12", "show_title": "1"},
        {"item_id": "B081YY3ZBV", "custom_label": "Shark Motor Head", "location": "rack A-12", "show_title": "1"},
        {"item_id": "HV320-BODY", "custom_label": "Vacuum Body", "location": "rack A-12", "show_title": "1"},
    ]
    return build_overlay_lines(sample_order, sample_rows, cfg)


def _order_link_for(platform: str, order_id: str) -> str:
    p = str(platform or "").strip().lower()
    oid = str(order_id or "").strip()
    if not oid:
        return ""
    if p == "amazon":
        return f"https://sellercentral.amazon.com/orders-v3/order/{oid}"
    if p == "ebay":
        return f"https://www.ebay.com/mesh/ord/details?orderid={oid}"
    return ""


def _batch_dirs() -> list[Path]:
    rows = [p for p in settings.processed_root_folder.glob("batch_*") if p.is_dir()]
    rows.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return rows


def _load_batch_report_rows(batch_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    report_path = batch_dir / "batch_report.json"
    if not report_path.exists():
        return {}, []
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return {}, []
    results = list(report.get("results", []) if isinstance(report, dict) else [])
    matched = [r for r in results if str(r.get("status", "")).lower() == "matched"]
    rows: list[dict[str, Any]] = []
    for i, r in enumerate(matched):
        order_id = str(r.get("order_id", "") or "").strip()
        platform = str(r.get("platform", "") or "").strip().lower()
        process_index = int(r.get("process_index", i) or i)
        qty = int(r.get("quantity_total", r.get("sort_qty", 0)) or 0)
        total_paid = r.get("total_paid", "")
        if isinstance(total_paid, (int, float)):
            total_paid = f"{float(total_paid):.2f}"
        item_keys = r.get("item_keys", []) or []
        if isinstance(item_keys, list):
            item_keys_text = ", ".join([str(x) for x in item_keys if str(x).strip()])
        else:
            item_keys_text = str(item_keys or "")
        item_titles = r.get("item_titles", []) or []
        if isinstance(item_titles, list):
            item_titles_text = " | ".join([str(x) for x in item_titles if str(x).strip()])
        else:
            item_titles_text = str(item_titles or "")
        rows.append(
            {
                "process_index": process_index,
                "order_id": order_id,
                "platform": platform,
                "name": str(r.get("ship_name", "") or ""),
                "zip": str(r.get("ship_postal", "") or ""),
                "tracking": str(r.get("tracking_number", "") or ""),
                "carrier": str(r.get("carrier", "") or ""),
                "qty": qty,
                "total_paid": str(total_paid or ""),
                "item_keys": item_keys_text,
                "item_titles": item_titles_text,
                "output_pdf": str(r.get("output_pdf", "") or ""),
                "order_link": _order_link_for(platform, order_id),
            }
        )
    rows.sort(key=lambda x: int(x.get("process_index", 0)))
    return report if isinstance(report, dict) else {}, rows


def create_app() -> FastAPI:
    app = FastAPI(title="Label Enricher")

    app.mount("/static", StaticFiles(directory=str(settings.resource_dir / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, msg: str = "", stale_open: int = 0, open_pdf: str = ""):
        status = batch_manager.scan_inputs()
        latest_batch = batch_manager.latest_batch_snapshot()
        reprocess_preview = batch_manager.latest_batch_reprocess_candidates()
        return _templates().TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "status": status,
                "message": msg,
                "log_lines": _tail_log(settings.logs_folder / "label_enricher.log"),
                "unresolved": _unresolved_for_ui(),
                "latest_batch": latest_batch,
                "reprocess_preview": reprocess_preview,
                "needs_review_count": _needs_review_count(),
                "needs_review_rows": _needs_review_rows(),
                "auto_added_review_count": _auto_added_review_count(),
                "stale_open": bool(stale_open),
                "open_pdf": open_pdf,
            },
        )

    @app.get("/batch-table", response_class=HTMLResponse)
    def batch_table_page(request: Request, batch: str = "latest", msg: str = ""):
        dirs = _batch_dirs()
        if not dirs:
            return _templates().TemplateResponse("batch_table.html", {"request": request, "message": _ui("No processed batches found."), "rows": [], "batch_options": [], "selected_batch": "", "summary": {}, "default_sort": "process_index"})

        selected_dir: Path
        if batch and batch != "latest":
            cand = settings.processed_root_folder / batch
            selected_dir = cand if cand.exists() else dirs[0]
        else:
            selected_dir = dirs[0]

        report, rows = _load_batch_report_rows(selected_dir)
        summary = report.get("summary", {}) if isinstance(report, dict) else {}
        batch_options = [{"name": p.name, "timestamp": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")} for p in dirs[:50]]

        return _templates().TemplateResponse(
            "batch_table.html",
            {
                "request": request,
                "message": msg,
                "rows": rows,
                "batch_options": batch_options,
                "selected_batch": selected_dir.name,
                "summary": summary,
                "default_sort": "process_index",
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
            return _redirect_ui("/", "Uploaded {count} file(s) to staging", count=saved)

        result = batch_manager.process_batch()
        if not result.get("ok"):
            return _redirect_ui("/", "Uploaded {count} file(s). Process failed: {error}", count=saved, error=result.get("error", _ui("Batch failed")))

        report = result.get("report", {})
        summary = report.get("summary", {})
        warnings = report.get("warnings", {}) if isinstance(report, dict) else {}
        parts = [
            _ui("Uploaded {count} file(s). Batch complete.", count=saved),
            _batch_counts_text(summary),
        ]
        sci_count = int((warnings or {}).get("scientific_notation_item_numbers", 0) or 0)
        if sci_count > 0:
            parts.append(_ui("Warning: eBay CSV item numbers looked spreadsheet-formatted on {count} row(s). Re-download the Orders Report from eBay and upload the original CSV without opening/saving it in Excel.", count=sci_count))

        unresolved_count, review_count = _queue_counts()
        if unresolved_count > 0:
            parts.append(_ui("Address {count} unprocessed label(s) before opening combined pdf.", count=unresolved_count))
            return _redirect_with_message("/unprocessed", _join_ui_parts(parts))
        if review_count > 0:
            parts.append(_ui("Address {count} items needing review before opening combined pdf.", count=review_count))
            return _redirect_with_message("/items/review", _join_ui_parts(parts))

        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok") and combined.get("path"):
                combined_path = Path(str(combined.get("path", "")))
                auto_print_enabled = _parse_bool(auto_print_after_process, False)
                if auto_print_enabled:
                    if _print_file(combined_path):
                        parts.append(_ui("Sent combined PDF to default printer"))
                    else:
                        parts.append(_ui("Auto-print only works with Adobe Acrobat."))
                else:
                    message = _join_ui_parts(parts + [_ui("Opened combined PDF ({count} files)", count=combined.get("count", 0))])
                    return _redirect_with_message(f"/?open_pdf={quote_plus(str(combined_path))}", message)
            else:
                parts.append(_ui("No combined PDF generated"))

        if int(summary.get("processed_slips", 0) or 0) > 0:
            parts.append(
                _ui(
                    "Packing slips synced: {processed} (items touched: {touched})",
                    processed=summary.get("processed_slips", 0),
                    touched=summary.get("synced_items", 0),
                )
            )

        return _redirect_with_message("/", _join_ui_parts(parts))

    @app.post("/staged/clear")
    def clear_staged_files():
        removed = batch_manager.clear_staged_files()
        return _redirect_ui("/", "Cleared {count} staged file(s)", count=removed)

    @app.post("/process")
    def process_batch():
        result = batch_manager.process_batch()
        if not result.get("ok"):
            return _redirect_with_message("/", str(result.get("error", _ui("Batch failed"))))
        report = result.get("report", {})
        summary = report.get("summary", {})
        warnings = report.get("warnings", {}) if isinstance(report, dict) else {}
        parts = [_ui("Batch complete."), _batch_counts_text(summary)]
        sci_count = int((warnings or {}).get("scientific_notation_item_numbers", 0) or 0)
        if sci_count > 0:
            parts.append(_ui("Warning: eBay CSV item numbers looked spreadsheet-formatted on {count} row(s). Re-download the Orders Report from eBay and upload the original CSV without opening/saving it in Excel.", count=sci_count))
        unresolved_count, review_count = _queue_counts()
        if unresolved_count > 0:
            parts.append(_ui("Address {count} unprocessed label(s) before opening combined pdf.", count=unresolved_count))
            return _redirect_with_message("/unprocessed", _join_ui_parts(parts))
        if review_count > 0:
            parts.append(_ui("Address {count} items needing review before opening combined pdf.", count=review_count))
            return _redirect_with_message("/items/review", _join_ui_parts(parts))
        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok"):
                parts.append(_ui("Combined PDF ready ({count} files)", count=combined.get("count", 0)))
        if int(summary.get("processed_slips", 0) or 0) > 0:
            parts.append(
                _ui(
                    "Packing slips synced: {processed} (items touched: {touched})",
                    processed=summary.get("processed_slips", 0),
                    touched=summary.get("synced_items", 0),
                )
            )
        return _redirect_with_message("/", _join_ui_parts(parts))

    @app.get("/process/reprocess-latest/confirm", response_class=HTMLResponse)
    def reprocess_latest_confirm_page(request: Request, msg: str = ""):
        preview = batch_manager.latest_batch_reprocess_candidates()
        names = list(preview.get("restage_preview_names", []) if isinstance(preview, dict) else [])
        total = int(preview.get("restage_file_count", 0) if isinstance(preview, dict) else 0)
        shown = names[:40]
        more_count = max(0, total - len(shown))
        return _templates().TemplateResponse(
            "reprocess_confirm.html",
            {
                "request": request,
                "message": msg,
                "preview": preview,
                "preview_names": shown,
                "preview_more_count": more_count,
            },
        )

    @app.post("/process/reprocess-latest")
    def reprocess_latest_batch():
        result = batch_manager.reprocess_latest_batch()
        if not result.get("ok"):
            return _redirect_with_message("/", str(result.get("error", _ui("Reprocess failed"))))

        summary = result.get("report", {}).get("summary", {})
        parts = [
            _ui("Reprocessed previous batch. Restaged: {count} file(s).", count=result.get("restaged_files", 0)),
            _batch_counts_text(summary),
        ]
        if int(summary.get("matched", 0) or 0) > 0:
            combined = batch_manager.combine_latest_output_pdfs()
            if combined.get("ok"):
                parts.append(_ui("Combined PDF ready ({count} files)", count=combined.get("count", 0)))
        return _redirect_with_message("/", _join_ui_parts(parts))

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
            return _redirect_with_message("/reprocess-select", str(result.get("error", _ui("Reprocess failed"))))

        summary = result.get("report", {}).get("summary", {})
        parts = [
            _ui("Reprocessed selected labels. Selected: {selected}.", selected=result.get("selected_labels", len(selected_order_ids))),
            _batch_counts_text(summary),
        ]

        combined = result.get("combined", {})
        if isinstance(combined, dict) and combined.get("ok"):
            settings.open_folder(Path(str(combined.get("path", ""))))
            parts.append(_ui("Opened combined PDF ({count} files)", count=combined.get("count", 0)))

        return _redirect_with_message("/reprocess-select", _join_ui_parts(parts))

    @app.post("/batch/combine-latest")
    def combine_latest():
        result = batch_manager.combine_latest_output_pdfs()
        if not result.get("ok"):
            return _redirect_with_message("/", str(result.get("error", _ui("Combine failed"))))
        return _redirect_ui("/", "Combined {count} PDFs: {path}", count=result.get('count', 0), path=result.get('path', ''))

    @app.post("/batch/open-combined-latest")
    def open_combined_latest(force_open: str | None = Form(None), reprocess_if_stale: str | None = Form(None)):
        guard = _queue_guard_redirect("opening combined pdf")
        if guard is not None:
            return guard

        stale = _settings_changed_since_latest_batch()
        if stale and not _parse_bool(force_open, False) and not _parse_bool(reprocess_if_stale, False):
            return _redirect_with_message(
                "/?stale_open=1",
                _ui("Layout settings or items.csv changed since last batch. Reprocess before opening for accurate output."),
                status_code=303,
            )

        if stale and _parse_bool(reprocess_if_stale, False):
            processed = batch_manager.reprocess_latest_batch()
            if not processed.get("ok"):
                return _redirect_ui("/", "Reprocess failed: {error}", error=processed.get("error", _ui("Batch failed")))
            summary = processed.get("report", {}).get("summary", {})
            if int(summary.get("matched", 0) or 0) <= 0:
                return _redirect_ui("/", "Reprocess completed but no matched labels were generated")

        snap = batch_manager.latest_batch_snapshot()
        path = snap.get("combined_pdf", "") if isinstance(snap, dict) else ""
        if not path or (stale and _parse_bool(reprocess_if_stale, False)):
            result = batch_manager.combine_latest_output_pdfs()
            if not result.get("ok"):
                return _redirect_with_message("/", str(result.get("error", _ui("Combine failed"))))
            path = result.get("path", "")

        if path:
            return _redirect_with_message(f"/?open_pdf={quote_plus(str(path))}", _ui("Opened combined PDF: {path}", path=path))
        return _redirect_ui("/", "No combined PDF available")

    @app.post("/open")
    def open_folder(request: Request, target: str = Form(...)):
        ok = False
        if target == "incoming":
            ok = settings.open_folder(settings.incoming_batch_folder)
        elif target == "processed":
            snap = batch_manager.latest_batch_snapshot()
            latest = Path(snap["batch_dir"]) if isinstance(snap, dict) and snap.get("batch_dir") else settings.processed_root_folder
            ok = settings.open_folder(latest)
        elif target == "app_root":
            ok = settings.open_folder(settings.base_dir)

        is_ajax = request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"
        if is_ajax:
            return JSONResponse({"ok": ok, "message": _ui("Opened folder") if ok else _ui("Could not open folder from app")}, status_code=(200 if ok else 500))
        if ok:
            return _redirect_ui("/", "Opened folder")
        return _redirect_ui("/", "Could not open folder from app")

    @app.post("/theme/comic-toggle")
    def theme_comic_toggle(request: Request):
        cfg = settings.config
        ui_cfg = cfg.setdefault("ui", {})
        cur_font = str(ui_cfg.get("font_mode", "default") or "default")
        next_font = "default" if cur_font == "comic" else "comic"
        ui_cfg["font_mode"] = next_font
        ui_cfg["comic_mode"] = (next_font == "comic")
        cfg.setdefault("print_layout", {})["comic_mode"] = False
        settings.save(cfg)
        dest = request.headers.get("referer") or "/"
        state = "ON" if next_font == "comic" else "OFF"
        return _redirect_ui(dest, "Comic Mode {state}", state=state)

    @app.post("/app/close")
    def close_app():
        _launch_stop_script()
        _schedule_shutdown(0.2)
        return Response(status_code=204)

    @app.get("/batch/view-pdf")
    def view_batch_pdf(path: str = Query(""), msg: str = Query("")):
        candidate = Path(path)
        if not path or not candidate.exists() or candidate.suffix.lower() != ".pdf":
            return _redirect_ui("/", "No combined PDF available")
        return FileResponse(path=str(candidate), filename=candidate.name, media_type="application/pdf", headers={"Content-Disposition": f'inline; filename="{candidate.name}"'})

    @app.get("/preview", response_class=HTMLResponse)
    def preview_page():
        return RedirectResponse(url="/settings", status_code=302)

    @app.get("/settings/live-preview.png")
    def settings_live_preview(
        label_pdf: str = Query(""),
        margin_direction: str = Query("top_bottom"),
        margin_mode: str = Query("both"),
        font_size: int = Query(14),
        backside_font_size: int = Query(20),
        line_spacing: int = Query(18),
        backside_line_spacing: int = Query(24),
        strip_thickness: int = Query(32),
        edge_padding: int = Query(8),
        side_padding: int = Query(8),
        spill_edge_padding: int = Query(8),
        spill_side_padding: int = Query(8),
        spill_strip_thickness: int = Query(32),
        spill_font_size: int = Query(11),
        spill_line_spacing: int = Query(14),
        compact_threshold: int = Query(4),
        summary_page_min_items: int = Query(4),
        summary_page_mode: str = Query("half_page"),
        summary_page_orientation: str = Query("normal"),
        summary_page_font_size: int = Query(20),
        summary_page_line_spacing: int = Query(24),
        summary_page_wrap_mode: str = Query("word"),
        summary_page_text_align: str = Query("left"),
        summary_page_margin: int = Query(24),
        total_display_mode: str = Query("grand_total"),
        text_align: str = Query("left"),
        wrap_mode: str = Query("word"),
        line_layout_mode: str = Query("qty_label_loc_inline"),
        field_order_csv: str = Query("label,qty,total,location,title"),
        inline_fields_csv: str = Query("qty,label,location"),
        line_groups_csv: str = Query(""),
        inline_separator: str = Query(" | "),
        show_field_labels: str | None = Query("1"),
        page_mode: str = Query("half_sheet_top"),
        render_mode: str = Query("margin"),
    ):
        preview_pdf: Path | None = None
        try:
            src = Path(label_pdf)
            if not label_pdf or not src.exists():
                return Response(content=b"", media_type="image/png")

            cfg = _build_preview_config(
                margin_direction=margin_direction,
                margin_mode=margin_mode,
                font_size=font_size,
                backside_font_size=backside_font_size,
                line_spacing=line_spacing,
                backside_line_spacing=backside_line_spacing,
                strip_thickness=strip_thickness,
                edge_padding=edge_padding,
                side_padding=side_padding,
                spill_edge_padding=spill_edge_padding,
                spill_side_padding=spill_side_padding,
                spill_strip_thickness=spill_strip_thickness,
                spill_font_size=spill_font_size,
                spill_line_spacing=spill_line_spacing,
                compact_threshold=compact_threshold,
                summary_page_min_items=summary_page_min_items,
                summary_page_mode=summary_page_mode,
                summary_page_orientation=summary_page_orientation,
                summary_page_font_size=summary_page_font_size,
                summary_page_line_spacing=summary_page_line_spacing,
                summary_page_wrap_mode=summary_page_wrap_mode,
                summary_page_text_align=summary_page_text_align,
                summary_page_margin=summary_page_margin,
                total_display_mode=total_display_mode,
                text_align=text_align,
                wrap_mode=wrap_mode,
                line_layout_mode=line_layout_mode,
                field_order_csv=field_order_csv,
                inline_fields_csv=inline_fields_csv,
                line_groups_csv=line_groups_csv,
                inline_separator=inline_separator,
                show_field_labels=_parse_bool(show_field_labels, True),
                page_mode=page_mode,
                render_mode=render_mode,
            )

            field_order_csv, inline_fields_csv = _apply_line_layout_mode(line_layout_mode, field_order_csv, inline_fields_csv)
            effective_line_groups = (line_groups_csv or "").strip() if (line_layout_mode or "").strip().lower() == "custom" else _line_groups_for_mode(line_layout_mode)
            lines = _sample_lines_for_order(field_order_csv, inline_fields_csv, _parse_bool(show_field_labels, True), inline_separator, effective_line_groups)

            settings.processed_root_folder.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=str(settings.processed_root_folder), prefix="_live_preview_", suffix=".pdf", delete=False) as tmp:
                preview_pdf = Path(tmp.name)

            page_w, page_h = get_page_size(src)
            if render_mode == "backside" and str(page_mode).startswith("half_sheet_"):
                panel_overlay, _ = create_info_panel_overlay_pdf(page_w, page_h, lines, cfg, draw_rect=True)
                merge_overlays_on_first_page(src, [panel_overlay], preview_pdf)
            else:
                primary_overlay, remaining = create_overlay_pdf(page_w, page_h, lines, cfg, draw_rect=True, region="primary")
                overlays = [primary_overlay]
                if margin_mode == "both" and render_mode != "backside":
                    secondary_lines = remaining if remaining else []
                    secondary_overlay, still_remaining = create_overlay_pdf(page_w, page_h, secondary_lines, cfg, draw_rect=True, region="secondary")
                    overlays.append(secondary_overlay)

                    layout = cfg.get("print_layout", {})
                    orientation = str(layout.get("orientation_mode", "normal"))
                    primary_preset = str(layout.get("rotated_primary_preset", layout.get("placement_preset", "")))
                    secondary_preset = str(layout.get("rotated_secondary_preset", ""))
                    side_pair = {primary_preset, secondary_preset} == {"left_margin", "right_margin"}

                    if orientation == "rotated_90" and side_pair:
                        spill_cfg = copy.deepcopy(cfg)
                        spill_layout = spill_cfg.setdefault("print_layout", {})
                        spill_layout["orientation_mode"] = "normal"
                        spill_layout["margin_box_height"] = int(layout.get("spill_margin_box_height", layout.get("margin_box_height", 36)))
                        spill_layout["edge_inset_x"] = float(layout.get("edge_inset_x", 8)) + float(layout.get("margin_box_width", 32)) + float(layout.get("spill_edge_inset_x", 8))
                        spill_layout["edge_inset_y"] = float(layout.get("spill_edge_inset_y", layout.get("edge_inset_y", 24)))
                        spill_layout["font_size"] = int(layout.get("spill_font_size", layout.get("font_size", 11)))
                        spill_layout["line_spacing"] = int(layout.get("spill_line_spacing", layout.get("line_spacing", 14)))

                        top_spill_overlay, top_remaining = create_overlay_pdf(
                            page_w,
                            page_h,
                            still_remaining if still_remaining else [],
                            spill_cfg,
                            draw_rect=True,
                            region="primary",
                            preset_override="top_margin",
                        )
                        overlays.append(top_spill_overlay)

                        bottom_spill_overlay, _ = create_overlay_pdf(
                            page_w,
                            page_h,
                            top_remaining if top_remaining else [],
                            spill_cfg,
                            draw_rect=True,
                            region="primary",
                            preset_override="bottom_margin",
                        )
                        overlays.append(bottom_spill_overlay)

                if render_mode == "both":
                    panel_overlay, _ = create_info_panel_overlay_pdf(page_w, page_h, lines, cfg, draw_rect=True)
                    overlays.append(panel_overlay)
                merge_overlays_on_first_page(src, overlays, preview_pdf)

            with fitz.open(str(preview_pdf)) as doc:
                pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
                return Response(content=pix.tobytes("png"), media_type="image/png")
        except Exception:
            logging.exception("Failed to generate live preview for %s", label_pdf)
            return Response(content=b"", media_type="image/png")
        finally:
            if preview_pdf and preview_pdf.exists():
                try:
                    preview_pdf.unlink()
                except Exception:
                    pass
    @app.get("/items", response_class=HTMLResponse)
    def items_page(request: Request, msg: str = ""):
        rows = _rows_with_keys(item_db.load_rows())
        staged_sync = _load_items_sync_stage()
        label_hints = _load_label_hints()
        hint_matches, merge_candidates = _build_items_assist(rows, label_hints)
        return _templates().TemplateResponse("items.html", {"request": request, "rows": rows, "message": msg, "backups_count": _items_backup_count(), "page_mode": "items", "label_hints": label_hints, "hint_matches": hint_matches, "merge_candidates": merge_candidates, "staged_sync": staged_sync, "app_root_path": str(settings.base_dir), "items_csv_mtime_iso": _safe_items_mtime_iso(), "items_csv_path": str(settings.items_csv_path), "auto_added_review_count": _auto_added_review_count()})

    @app.get("/items/review", response_class=HTMLResponse)
    def items_review_page(request: Request, msg: str = ""):
        rows = _rows_with_keys([r for r in item_db.load_rows() if str(r.get("needs_review", "0")).strip() == "1"])
        label_hints = _load_label_hints()
        hint_matches, merge_candidates = _build_items_assist(rows, label_hints)
        return _templates().TemplateResponse("items.html", {"request": request, "rows": rows, "message": msg, "backups_count": _items_backup_count(), "page_mode": "review", "label_hints": label_hints, "hint_matches": hint_matches, "merge_candidates": merge_candidates, "app_root_path": str(settings.base_dir), "items_csv_mtime_iso": _safe_items_mtime_iso(), "items_csv_path": str(settings.items_csv_path), "auto_added_review_count": _auto_added_review_count()})

    @app.post("/items/save")
    async def items_save(request: Request):
        form = await request.form()
        source_page = str(form.get("source_page", "items") or "items").strip().lower()
        target = "/items/review" if source_page == "review" else "/items"
        try:
            kept, deleted = item_db.update_rows_from_form(dict(form))
            if source_page == "review":
                remaining = _needs_review_count()
                return _redirect_ui(target, "Saved review changes. Remaining items needing review: {remaining}", remaining=remaining)
            return _redirect_ui(target, "Saved {kept} row(s); deleted {deleted} row(s)", kept=kept, deleted=deleted)
        except PermissionError:
            return _redirect_ui(
                target,
                "Could not save items.csv. Please close items.csv in Excel and try again",
            )
        except OSError as exc:
            return _redirect_ui(target, "Could not save items.csv: {error}", error=type(exc).__name__)

    @app.post("/items/apply-hint")
    async def items_apply_hint(request: Request, row_key: str = Form(...), source_page: str = Form("items")):
        target = "/items/review" if str(source_page or "").strip().lower() == "review" else "/items"
        row = next((r for r in _rows_with_keys(item_db.load_rows()) if str(r.get("_row_key", "") or "") == row_key), None)
        is_ajax = request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"
        if row is None:
            if is_ajax:
                return JSONResponse({"ok": False, "error": _ui("Could not find that item row")}, status_code=404)
            return _redirect_ui(target, "Could not find that item row")
        hint = _find_hint_for_row(row, _load_label_hints())
        if hint is None:
            if is_ajax:
                return JSONResponse({"ok": False, "error": _ui("No matching hint found for that row")}, status_code=404)
            return _redirect_ui(target, "No matching hint found for that row")
        updated: dict[str, str] = {}
        label = str(hint.get("label", "") or "").strip()
        location = str(hint.get("location", "") or "").strip()
        ebay = item_db._extract_ebay_id(str(hint.get("ebay_item_number", "") or ""))
        asin = item_db._extract_asin_any(str(hint.get("asin", "") or ""), "")
        if label:
            updated["custom_label"] = label
        if location and not str(row.get("location", "") or "").strip():
            updated["location"] = location
        if ebay and not str(row.get("ebay_item_number", "") or "").strip():
            updated["ebay_item_number"] = ebay
        if asin and not str(row.get("amazon_asin", "") or "").strip():
            updated["amazon_asin"] = asin
        if not updated:
            if is_ajax:
                return JSONResponse({"ok": False, "error": _ui("Hint matched, but there was nothing new to fill in")}, status_code=200)
            return _redirect_ui(target, "Hint matched, but there was nothing new to fill in")
        updated["platform"] = str(row.get("platform", "") or "")
        updated["item_title"] = str(row.get("item_title", "") or "")
        if is_ajax:
            return JSONResponse({"ok": True, "message": _ui("Applied hint to item row"), "row_key": row_key, "updated": updated})
        return _redirect_ui(target, "Applied hint to item row")

    @app.post("/items/undo-hint")
    async def items_undo_hint(
        request: Request,
        row_key: str = Form(...),
        custom_label: str = Form(""),
        location: str = Form(""),
        ebay_item_number: str = Form(""),
        amazon_asin: str = Form(""),
        source_page: str = Form("items"),
    ):
        target = "/items/review" if str(source_page or "").strip().lower() == "review" else "/items"
        previous = {
            "custom_label": custom_label,
            "location": location,
            "ebay_item_number": ebay_item_number,
            "amazon_asin": amazon_asin,
        }
        restored = item_db.restore_hint_fields(row_key, previous)
        is_ajax = request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"
        if not restored:
            if is_ajax:
                return JSONResponse({"ok": False, "error": _ui("Nothing to undo for that hint")}, status_code=200)
            return _redirect_ui(target, "Nothing to undo for that hint")
        if is_ajax:
            return JSONResponse({"ok": True, "message": _ui("Undid hint changes"), "updated": restored, "row_key": row_key})
        return _redirect_ui(target, "Undid hint changes")

    @app.get("/items/merge-preview")
    def items_merge_preview(row_key: str = Query(...), target_key: str = Query(...)):
        preview = item_db.preview_merge_by_keys(row_key, target_key)
        if preview is None:
            return JSONResponse({"ok": False, "error": _ui("Could not build merge preview")}, status_code=404)
        return JSONResponse({"ok": True, "preview": preview})

    @app.post("/items/merge")
    def items_merge(row_key: str = Form(...), target_key: str = Form(...), source_page: str = Form("items")):
        target = "/items/review" if str(source_page or "").strip().lower() == "review" else "/items"
        preview = item_db.preview_merge_by_keys(row_key, target_key)
        if preview is None:
            return _redirect_ui(target, "Could not preview merge; one of the rows no longer exists")
        ok = item_db.merge_rows_by_keys(row_key, target_key)
        if not ok:
            return _redirect_ui(target, "Could not merge those rows")
        merged = preview.get("merged") or {}
        return _redirect_ui(target, "Merged into {label}", label=_row_display_name(merged))

    @app.get("/items/export")
    def items_export():
        if not settings.items_csv_path.exists():
            return _redirect_ui("/items", "items.csv not found")
        return FileResponse(path=str(settings.items_csv_path), filename="items.csv", media_type="text/csv")

    @app.post("/items/backup")
    def items_backup_now(from_page: str = Form("items")):
        backup = item_db.create_backup_now()
        if backup:
            msg = _ui("Backup created: {name} in {folder}", name=backup.name, folder=backup.parent)
        else:
            msg = _ui("No backup created")
        if from_page == "dashboard":
            return _redirect_with_message("/", msg)
        return _redirect_with_message("/items", msg)

    @app.post("/items/open-backups")
    def items_open_backups(request: Request, from_page: str = Form("items")):
        ok = settings.open_folder(item_db.backups_dir)
        msg = _ui("Opened items backups folder") if ok else _ui("Could not open backups folder")
        is_ajax = request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"
        if is_ajax:
            return JSONResponse({"ok": ok, "message": msg}, status_code=(200 if ok else 500))
        if from_page == "dashboard":
            return _redirect_with_message("/", msg)
        return _redirect_with_message("/items", msg)

    @app.post("/items/clear-needs-review")
    def items_clear_needs_review(from_page: str = Form("dashboard"), mode: str = Form("clear_flags")):
        result = item_db.clear_needs_review_with_mode(mode)
        cleared = int(result.get("cleared", 0))
        deleted = int(result.get("deleted", 0))
        parts: list[str] = []
        if cleared:
            parts.append(_ui("cleared {count} review flag(s)", count=cleared))
        if deleted:
            parts.append(_ui("deleted {count} auto-added row(s)", count=deleted))
        if not parts:
            parts.append(_ui("no review rows changed"))
        msg = _ui("Needs review update: {details}", details=", ".join(parts))
        if from_page == "items":
            return _redirect_with_message("/items", msg)
        if from_page == "review":
            return _redirect_with_message("/items/review", msg)
        return _redirect_with_message("/", msg)

    @app.post("/items/replace")
    async def items_replace_items_csv(items_csv_file: UploadFile = File(...)):
        filename = Path(str(items_csv_file.filename or "items.csv")).name
        suffix = Path(filename).suffix.lower()
        if suffix not in (".csv", ".txt", ".tsv"):
            return _redirect_ui("/items", "Upload a CSV/TXT/TSV file to replace items.csv")

        temp = settings.incoming_batch_folder / f"_items_replace_{filename}"
        with temp.open("wb") as f:
            shutil.copyfileobj(items_csv_file.file, f)

        backup = item_db.create_backup_now()
        try:
            shutil.copyfile(temp, settings.items_csv_path)
            item_db.load_rows()
            backup_name = backup.name if backup else _ui("no backup")
            msg = _ui("Replaced items.csv with {filename}. Previous file backed up as {backup_name}.", filename=filename, backup_name=backup_name)
        except PermissionError:
            msg = _ui("Could not replace items.csv. Please close items.csv in Excel and try again")
        except OSError:
            msg = _ui("Could not replace items.csv: {error}", error="OSError")
        except Exception as exc:
            msg = _ui("Could not replace items.csv: {error}", error=type(exc).__name__)
        finally:
            try:
                temp.unlink(missing_ok=True)
            except Exception:
                pass
        return _redirect_with_message("/items", msg)

    @app.post("/items/sync-stage")
    async def items_sync_stage(master_csv_stage: UploadFile = File(...)):
        temp = settings.incoming_batch_folder / f"_sync_stage_{master_csv_stage.filename}"
        with temp.open("wb") as f:
            shutil.copyfileobj(master_csv_stage.file, f)
        try:
            preview = item_db.preview_sync_from_master_csv(temp)
            staged = {
                "source_filename": master_csv_stage.filename or "",
                "created_at": datetime.now().isoformat(),
                "counts": preview.get("counts", {}),
                "entries": preview.get("entries", []),
            }
            _save_items_sync_stage(staged)
            cnt = int((staged.get("counts") or {}).get("total", 0))
            return _redirect_ui("/items", "Staged {count} import row(s) for review", count=cnt)
        finally:
            try:
                temp.unlink(missing_ok=True)
            except Exception:
                pass
    @app.post("/items/sync-apply")
    def items_sync_apply(only_add_new: str = Form("0")):
        staged = _load_items_sync_stage()
        if not staged:
            return _redirect_ui("/items", "No staged import found")
        entries = list(staged.get("entries") or [])
        only_new = str(only_add_new).strip().lower() in ("1", "on", "true", "yes")
        result = item_db.apply_staged_sync(entries, only_add_new=only_new)
        _clear_items_sync_stage()
        return _redirect_ui(
            "/items",
            "Applied staged import: created {created}, updated {updated}, skipped {skipped}",
            created=result.get("created", 0),
            updated=result.get("updated", 0),
            skipped=result.get("skipped", 0),
        )

    @app.post("/items/sync-clear")
    def items_sync_clear():
        _clear_items_sync_stage()
        return _redirect_ui("/items", "Cleared staged import")

    @app.post("/hints/upload")
    async def hints_upload(
        hints_csv: UploadFile = File(...),
        from_page: str = Form("items"),
    ):
        target = "/items" if from_page == "items" else "/manual-entry"
        filename = str(hints_csv.filename or "").strip().lower()
        if not filename or not filename.endswith((".csv", ".tsv", ".txt")):
            return _redirect_ui(target, "Upload a CSV/TXT/TSV file for label/location hints")
        try:
            raw = await hints_csv.read()
            text = raw.decode("utf-8-sig", errors="ignore")
            if not text.strip():
                return _redirect_ui(target, "Hints file was empty")
            sample = text[:4096]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = "\t" if "\t" in sample else ","
            reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
            fieldnames = reader.fieldnames or []
            keymap = {_norm_hint_header(h or ""): (h or "") for h in fieldnames}

            def pick(*aliases: str) -> str | None:
                for a in aliases:
                    hit = keymap.get(_norm_hint_header(a))
                    if hit is not None:
                        return hit
                return None

            col_label = pick("internal label", "custom label", "label", "model", "item label")
            col_location = pick("location", "picking location")
            col_asin = pick("asin", "amazon asin", "amz asin")
            col_amazon_link = pick("amazon link", "amz link")
            col_ebay_item = pick("ebay item #", "ebay item", "ebay item number", "item #")
            col_ebay_link = pick("ebay link")
            if col_label is None and fieldnames:
                col_label = fieldnames[0]
            if col_location is None and len(fieldnames) >= 2:
                col_location = fieldnames[1]
            if col_asin is None and len(fieldnames) >= 3:
                col_asin = fieldnames[2]

            rows: list[dict[str, str]] = []
            for row in reader:
                rows.append({
                    "label": str((row or {}).get(col_label or "", "") or ""),
                    "location": str((row or {}).get(col_location or "", "") or ""),
                    "asin": str((row or {}).get(col_asin or "", "") or (row or {}).get(col_amazon_link or "", "") or ""),
                    "ebay_item_number": str((row or {}).get(col_ebay_item or "", "") or (row or {}).get(col_ebay_link or "", "") or ""),
                })
            saved = _save_label_hints(rows)
            return _redirect_ui(target, "Saved {count} label/location hint row(s)", count=saved)
        except Exception as ex:
            return _redirect_ui(target, "Could not upload label/location hints: {error}", error=ex)

    @app.post("/hints/clear")
    def hints_clear(from_page: str = Form("items")):
        try:
            _label_hints_path().unlink(missing_ok=True)
            msg = _ui("Cleared label/location hints")
        except Exception:
            msg = _ui("Could not clear label/location hints")
        url = "/items" if from_page == "items" else "/manual-entry"
        return _redirect_with_message(url, msg)


    def _render_manual_entry(request: Request, msg: str = "", prefill: dict[str, Any] | None = None):
        label_options = _manual_label_options()
        draft = _load_manual_entry_draft() if prefill is None else None
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
            "manual_prefix": "",
            "messy_text": "",
            "batch_messy_text": "",
            "write_to_items": "0",
            "batch_entries": _manual_batch_defaults(label_options),
        }
        if draft:
            defaults.update({k: v for k, v in draft.items() if v is not None})
        if prefill:
            defaults.update({k: v for k, v in prefill.items() if v is not None})
        batch_entries = [dict(row) for row in (defaults.get("batch_entries") or _manual_batch_defaults(label_options))]
        known_paths = {str(row.get("label_pdf", "") or "").strip() for row in batch_entries if str(row.get("label_pdf", "") or "").strip()}
        next_group = len({str(row.get("group_id", "") or "").strip() for row in batch_entries if str(row.get("group_id", "") or "").strip()}) + 1
        for opt in label_options:
            opt_path = str(opt.get("path", "") or "").strip()
            if not opt_path or opt_path in known_paths:
                continue
            batch_entries.append({
                "group_id": f"manual-group-{next_group:03d}",
                "label_pdf": opt_path,
                "platform": _manual_platform_for_label(opt_path, "amazon"),
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
                "manual_prefix": "",
                "messy_text": "",
            })
            known_paths.add(opt_path)
            next_group += 1
        defaults["batch_entries"] = _decorate_manual_batch_entries(batch_entries)

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
            },
        )

    @app.get("/manual-entry", response_class=HTMLResponse)
    def manual_entry_page(request: Request, msg: str = ""):
        return _render_manual_entry(request, msg=msg)

    @app.get("/manual-entry/item-lookup")
    def manual_entry_item_lookup(query: str = Query(""), platform: str = Query("amazon")):
        return JSONResponse({"ok": True, "rows": _manual_lookup_preview_rows(query, platform)})
    @app.post("/manual-entry/clear-staged")
    def manual_entry_clear_staged():
        _clear_manual_entry_draft()
        root = _manual_incoming_folder()
        removed = 0
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            try:
                p.unlink()
                removed += 1
            except Exception:
                pass
        try:
            shutil.rmtree(root / "_unzipped", ignore_errors=True)
            shutil.rmtree(root / "_split_pages", ignore_errors=True)
        except Exception:
            pass
        return _redirect_ui("/manual-entry", "Cleared {count} manual staged file(s)", count=removed)

    @app.post("/manual-entry/upload")
    async def manual_entry_upload(files: list[UploadFile] = File(...)):
        saved = 0
        manual_root = _manual_incoming_folder()
        for file in files:
            dest = _unique_path(manual_root, file.filename)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                shutil.copyfileobj(file.file, f)
            saved += 1
            if dest.suffix.lower() == ".zip":
                _extract_zip_files_into(manual_root)
        return _redirect_ui("/manual-entry", "Uploaded {count} file(s) to manual staging", count=saved)

    @app.post("/manual-entry/parse-text", response_class=HTMLResponse)
    async def manual_entry_parse_text(request: Request, messy_text: str = Form(""), label_pdf: str = Form("")):
        prefill = _apply_manual_db_prefill(_extract_manual_prefill_from_text(messy_text))
        prefill["messy_text"] = messy_text or ""
        if label_pdf:
            prefill["label_pdf"] = label_pdf
            prefill["platform"] = _manual_platform_for_label(label_pdf, str(prefill.get("platform", "amazon") or "amazon"))
        return _render_manual_entry(request, msg=_ui("Parsed text and pre-filled fields. Verify before creating output."), prefill=prefill)

    @app.post("/manual-entry/parse-text-batch", response_class=HTMLResponse)
    async def manual_entry_parse_text_batch(request: Request):
        form = await request.form()
        rows = _manual_rows_from_form(dict(form))

        parsed_count = 0
        batch_entries: list[dict[str, str]] = []
        for row in rows:
            expanded = _expand_manual_row_entries(row, parse_text=True)
            if str(row.get("messy_text", "") or "").strip():
                parsed_count += 1
            batch_entries.extend(expanded)

        batch_entries = _decorate_manual_batch_entries(batch_entries)
        prefill: dict[str, Any] = {
            "batch_entries": batch_entries,
            "write_to_items": "1" if bool(form.get("write_to_items")) else "0",
        }
        _save_manual_entry_draft(prefill)
        return _render_manual_entry(
            request,
            msg=_ui(
                "Parsed {parsed_count} row text block(s) into {row_count} manual row(s). Verify before creating.",
                parsed_count=parsed_count,
                row_count=len(batch_entries),
            ),
            prefill=prefill,
        )

    @app.post("/manual-entry/create-batch")
    async def manual_entry_create_batch(request: Request):
        form = await request.form()
        rows = _manual_rows_from_form(dict(form))
        write_to_items = bool(form.get("write_to_items"))
        auto_parse_text = bool(form.get("auto_parse_text"))
        open_combined = str(form.get("open_combined", "0") or "0").strip() == "1"

        if not rows:
            return _redirect_ui("/manual-entry", "No manual rows to process")

        expanded_rows: list[dict[str, str]] = []
        for row in rows:
            expanded_rows.extend(_expand_manual_row_entries(row, parse_text=auto_parse_text))
        rows = _decorate_manual_batch_entries(expanded_rows)

        _save_manual_entry_draft({
            "batch_entries": rows,
            "write_to_items": "1" if write_to_items else "0",
        })

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = settings.processed_root_folder / "manual_entries" / f"manual_batch_{ts}"
        out_dir.mkdir(parents=True, exist_ok=True)

        idx = item_db.index()
        rendered: list[Path] = []
        errors: list[str] = []
        grouped_rows: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            group_id = str(row.get("group_id", "") or "").strip() or f"manual-group-{len(grouped_rows) + 1:03d}"
            grouped_rows.setdefault(group_id, []).append(row)

        for group_index, group_rows in enumerate(grouped_rows.values(), start=1):
            lead = group_rows[0]
            label_pdf = next((str(r.get("label_pdf", "") or "").strip() for r in group_rows if str(r.get("label_pdf", "") or "").strip()), "")
            if not label_pdf:
                errors.append(_ui("Label {row}: Label PDF not found", row=group_index))
                continue

            source_paths = _resolve_manual_label_paths(label_pdf)
            if not source_paths:
                errors.append(_ui("Label {row}: Label PDF not found", row=group_index))
                continue
            src = source_paths[0]

            platform = _manual_platform_for_label(label_pdf, str(lead.get("platform", "amazon") or "amazon"))

            label_ref = next((str(r.get("label_ref", "") or "").strip() for r in group_rows if str(r.get("label_ref", "") or "").strip()), "") or _manual_label_ref_from_pdf(src)

            group_items: list[dict[str, Any]] = []
            order_total = 0.0
            explicit_total = False
            order_id = next((str(r.get("order_id", "") or "").strip() for r in group_rows if str(r.get("order_id", "") or "").strip()), "") or f"MANUAL-{ts}-{group_index:02d}"
            manual_prefix = next((str(r.get("manual_prefix", "") or "").strip() for r in group_rows if str(r.get("manual_prefix", "") or "").strip()), "")

            for item_index, r in enumerate(group_rows, start=1):
                key = str(r.get("item_key", "") or "").strip()
                asin = str(r.get("item_asin", "") or "").strip().upper()
                if platform == "amazon" and not asin and key.upper().startswith("B") and len(key) == 10:
                    asin = key.upper()

                try:
                    qty = int(float(str(r.get("quantity", "1") or "1")))
                except Exception:
                    qty = 0
                if qty <= 0:
                    errors.append(_ui("Label {row} item {item}: Quantity invalid", row=group_index, item=item_index))
                    continue

                title = str(r.get("title", "") or "").strip()
                label = str(r.get("custom_label", "") or "").strip()
                db_row = _manual_lookup_row(idx, platform, key, asin) if (key or asin) else None
                if not label and db_row is not None:
                    label = str(db_row.get("custom_label", "") or "").strip() or str(db_row.get("item_title", "") or "").strip()
                    if not title:
                        title = str(db_row.get("item_title", "") or "").strip()
                    if not str(r.get("location", "") or "").strip():
                        r["location"] = str(db_row.get("location", "") or "").strip()

                if not label and str(r.get("use_title_as_label", "1") or "1") != "0" and title:
                    label = title

                if not label and not key:
                    errors.append(_ui("Label {row} item {item}: Provide internal label or item key", row=group_index, item=item_index))
                    continue
                if not label:
                    errors.append(_ui("Label {row} item {item}: Could not derive internal label from key/title", row=group_index, item=item_index))
                    continue

                if write_to_items and key:
                    item_db.ensure_item(platform, key, title=title, item_asin=asin)
                    all_rows = item_db.load_rows()
                    for rr in all_rows:
                        aliases = {
                            str(rr.get("item_id", "") or "").strip(),
                            str(rr.get("ebay_item_number", "") or "").strip(),
                            str(rr.get("amazon_sku", "") or "").strip(),
                            str(rr.get("amazon_asin", "") or "").strip().upper(),
                        }
                        aliases = {a for a in aliases if a}
                        if key in aliases or (asin and asin in aliases):
                            rr["custom_label"] = label
                            if str(r.get("location", "") or "").strip():
                                rr["location"] = str(r.get("location", "") or "").strip()
                            rr["needs_review"] = "0"
                            if title and (platform == "amazon" or not str(rr.get("item_title", "")).strip()):
                                rr["item_title"] = title
                            break
                    item_db.save_rows(all_rows)
                    idx = item_db.index()

                line_total = _to_float(str(r.get("total_paid", "") or ""))
                if line_total > 0:
                    explicit_total = True
                    order_total = max(order_total, line_total)
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
                    "line_total": line_total,
                    "_manual_row": manual_row,
                }
                if platform == "amazon":
                    item["item_sku"] = key if not (key.upper().startswith("B") and len(key) == 10) else ""
                    item["item_asin"] = asin or (key.upper() if key.upper().startswith("B") and len(key) == 10 else "")
                else:
                    item["ebay_item_number"] = key
                group_items.append(item)

            if not group_items:
                continue

            order = {
                "platform": platform,
                "order_id": order_id,
                "ship_name": label_ref,
                "ship_postal": "",
                "tracking_number": label_ref,
                "items": group_items,
                "total_paid": order_total if explicit_total else sum(float(item.get("line_total", 0) or 0) for item in group_items),
                "source": "manual_entry_batch",
                "manual_prefix_text": manual_prefix,
            }

            for page_idx, page_src in enumerate(source_paths, start=1):
                page_order = dict(order)
                if len(source_paths) > 1:
                    page_order["order_id"] = f"{order_id}-p{page_idx:03d}"
                out_pdf = batch_manager._render_one_label(page_src, page_order, idx, out_dir, auto_add_missing_items=False)
                rendered.append(out_pdf)
                batch_manager.remove_unresolved_entry(str(Path(page_src)))
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
                    errors.append(_ui("Combine skipped: {name}", name=pdf_path.name))
            combined = out_dir / "combined_manual_output.pdf"
            with combined.open("wb") as f:
                writer.write(f)
            combined_opened = _open_file(combined) if open_combined else False

        parts = [_ui("Manual batch complete. Generated: {count}", count=len(rendered))]
        if errors:
            preview = "; ".join(errors[:3])
            if len(errors) > 3:
                preview += "; " + _ui("(+{count} more)", count=len(errors) - 3)
            parts.append(_ui("Errors: {count} | {preview}", count=len(errors), preview=preview))
        if rendered:
            parts.append(_ui("Opened combined manual PDF") if locals().get("combined_opened", False) else _ui("Created manual output PDF"))
        return _redirect_with_message("/manual-entry", _join_ui_parts(parts))

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

        source_paths = _resolve_manual_label_paths(label_pdf)
        if not source_paths:
            return _redirect_ui("/manual-entry", "Label PDF not found")
        src = source_paths[0]

        p = _manual_platform_for_label(label_pdf, platform)

        key = (item_key or "").strip()
        ref = (label_ref or "").strip()
        sig = extract_label_signals(src)
        if not ref:

            ref = str(sig.get("tracking_number", "") or sig.get("recipient_name", "") or "").strip()

            ref = _manual_label_ref_from_pdf(src)


        if int(quantity or 0) <= 0:
            return _redirect_ui("/manual-entry", "Quantity must be 1 or higher")

        asin = (item_asin or "").strip().upper()
        if p == "amazon" and not asin and key.upper().startswith("B") and len(key) == 10:
            asin = key.upper()

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
            return _redirect_ui("/manual-entry", "Provide internal label or item key")
        if not label:
            return _redirect_ui("/manual-entry", "Could not derive internal label from key/title")

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
        rendered_paths: list[Path] = []
        for page_idx, src in enumerate(source_paths, start=1):
            page_order = dict(order)
            if len(source_paths) > 1:
                page_order["order_id"] = f"{manual_order_id}-p{page_idx:03d}"
            rendered_paths.append(batch_manager._render_one_label(src, page_order, item_db.index(), out_dir, auto_add_missing_items=False))
            batch_manager.remove_unresolved_entry(str(Path(src)))
        _out_pdf = rendered_paths[0]
        opened = settings.open_folder(out_dir)

        batch_manager.remove_unresolved_entry(str(Path(label_pdf)))
        parts = [_ui("Created manual output PDF")]
        if opened:
            parts.append(_ui("Opened manual output PDF"))
        else:
            parts.append(_ui("Manual output PDF ready but could not auto-open"))
        return _redirect_with_message("/manual-entry", _join_ui_parts(parts))

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
        backside_font_size: int = Form(20),
        line_spacing: int = Form(18),
        backside_line_spacing: int = Form(24),
        strip_thickness: int = Form(32),
        edge_padding: int = Form(8),
        side_padding: int = Form(8),
        spill_edge_padding: int = Form(8),
        spill_side_padding: int = Form(8),
        spill_strip_thickness: int = Form(32),
        spill_font_size: int = Form(11),
        spill_line_spacing: int = Form(14),
        compact_threshold: int = Form(4),
        summary_page_min_items: int = Form(4),
        summary_page_mode: str = Form("half_page"),
        summary_page_orientation: str = Form("normal"),
        summary_page_font_size: int = Form(20),
        summary_page_line_spacing: int = Form(24),
        summary_page_wrap_mode: str = Form("word"),
        summary_page_text_align: str = Form("left"),
        summary_page_margin: int = Form(24),
        total_display_mode: str = Form("grand_total"),
        text_align: str = Form("left"),
        wrap_mode: str = Form("word"),
        line_layout_mode: str = Form("qty_label_loc_inline"),
        field_order_csv: str = Form("label,qty,total,location,title"),
        inline_fields_csv: str = Form("qty,label,location"),
        line_groups_csv: str = Form(""),
        inline_separator: str = Form(" | "),
        show_field_labels: str | None = Form(None),
        page_mode: str = Form("half_sheet_top"),
        render_mode: str = Form("margin"),
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
        sort_enable_carrier: str | None = Form(None),
        sort_dir_label: str = Form("asc"),
        sort_dir_qty: str = Form("asc"),
        sort_dir_item_key: str = Form("asc"),
        sort_dir_location: str = Form("asc"),
        sort_dir_carrier: str = Form("asc"),
        ui_language_mode: str = Form("en"),
        ui_font_mode: str = Form("default"),
    ):
        cfg = _build_preview_config(
            margin_direction=margin_direction,
            margin_mode=margin_mode,
            font_size=font_size,
            backside_font_size=backside_font_size,
            line_spacing=line_spacing,
            backside_line_spacing=backside_line_spacing,
            strip_thickness=strip_thickness,
            edge_padding=edge_padding,
            side_padding=side_padding,
            spill_edge_padding=spill_edge_padding,
            spill_side_padding=spill_side_padding,
            spill_strip_thickness=spill_strip_thickness,
            spill_font_size=spill_font_size,
            spill_line_spacing=spill_line_spacing,
            compact_threshold=compact_threshold,
            summary_page_min_items=summary_page_min_items,
            summary_page_mode=summary_page_mode,
            summary_page_orientation=summary_page_orientation,
            summary_page_font_size=summary_page_font_size,
            summary_page_line_spacing=summary_page_line_spacing,
            summary_page_wrap_mode=summary_page_wrap_mode,
            summary_page_text_align=summary_page_text_align,
            summary_page_margin=summary_page_margin,
                total_display_mode=total_display_mode,
                text_align=text_align,
            wrap_mode=wrap_mode,
            line_layout_mode=line_layout_mode,
            field_order_csv=field_order_csv,
            inline_fields_csv=inline_fields_csv,
            line_groups_csv=line_groups_csv,
            inline_separator=inline_separator,
            show_field_labels=bool(show_field_labels),
            page_mode=page_mode,
            render_mode=render_mode,
            output_sort_mode=output_sort_mode,
            sort_priority_1=sort_priority_1,
            sort_priority_2=sort_priority_2,
            sort_priority_3=sort_priority_3,
            sort_priority_4=sort_priority_4,
            sort_enable_label=bool(sort_enable_label),
            sort_enable_qty=bool(sort_enable_qty),
            sort_enable_item_key=bool(sort_enable_item_key),
            sort_enable_location=bool(sort_enable_location),
            sort_enable_carrier=bool(sort_enable_carrier),
            sort_dir_label=sort_dir_label,
            sort_dir_qty=sort_dir_qty,
            sort_dir_item_key=sort_dir_item_key,
            sort_dir_location=sort_dir_location,
            sort_dir_carrier=sort_dir_carrier,
        )
        cfg["admin"]["archive_retention_days"] = int(archive_retention_days)
        ui_cfg = cfg.setdefault("ui", {})
        if ui_language_mode not in ("en", "ko", "zh_cn", "zh_tw"):
            ui_language_mode = "en"
        if ui_font_mode not in ("default", "comic", "wingdings"):
            ui_font_mode = "default"
        ui_cfg["language_mode"] = ui_language_mode
        ui_cfg["font_mode"] = ui_font_mode
        ui_cfg["comic_mode"] = (ui_font_mode == "comic")
        cfg.setdefault("print_layout", {})["comic_mode"] = False
        settings.save(cfg)
        return _redirect_ui("/settings", "Saved")

    @app.post("/archives/purge")
    def purge_archives(days: int = Form(14)):
        removed = batch_manager.purge_archives(days)
        return _redirect_ui("/", "Purged {count} old batches", count=removed)

    @app.post("/resolve/clear")
    def resolve_clear(from_page: str = Form("unprocessed")):
        target = "/" if from_page == "dashboard" else "/unprocessed"
        try:
            removed = batch_manager.clear_unresolved_queue()
            msg = _ui("Cleared {count} unprocessed queue item(s).", count=removed)
        except Exception as ex:
            msg = _ui("Failed to clear unprocessed queue: {error}", error=ex)
        return _redirect_with_message(target, msg)

    @app.get("/resolve", response_class=HTMLResponse)
    def resolve_page(request: Request, msg: str = ""):
        rows = _unresolved_for_ui()
        variation_rows = [r for r in rows if str(r.get("reason", "")) == "multi_variation_choice_required"]
        other_rows = [r for r in rows if str(r.get("reason", "")) != "multi_variation_choice_required"]
        return _templates().TemplateResponse("resolve_match.html", {"request": request, "rows": rows, "variation_rows": variation_rows, "other_rows": other_rows, "message": msg})

    @app.get("/unprocessed", response_class=HTMLResponse)
    def unprocessed_page(request: Request, msg: str = ""):
        rows = _unresolved_for_ui()
        variation_rows = [r for r in rows if str(r.get("reason", "")) == "multi_variation_choice_required"]
        other_rows = [r for r in rows if str(r.get("reason", "")) != "multi_variation_choice_required"]
        return _templates().TemplateResponse("resolve_match.html", {"request": request, "rows": rows, "variation_rows": variation_rows, "other_rows": other_rows, "message": msg})

    @app.post("/resolve/variation-choice")
    async def resolve_variation_choice(label_pdf: str = Form(...), order_id: str = Form(...), variant_choice: str = Form(...)):
        result = batch_manager.save_variation_choice(label_pdf, order_id, variant_choice)
        if result.get("ok"):
            return _redirect_ui("/unprocessed", "Variation choice saved")
        return _redirect_with_message("/unprocessed", str(result.get("error", _ui("Could not save variation choice"))))

    @app.post("/resolve/save-all-variations")
    async def resolve_save_all_variations(request: Request):
        form = await request.form()
        labels = list(form.getlist("label_pdf"))
        orders = list(form.getlist("order_id"))
        variants = list(form.getlist("variant_choice"))
        choices: list[dict[str, str]] = []
        for i in range(min(len(labels), len(orders), len(variants))):
            choices.append({
                "label_pdf": str(labels[i] or ""),
                "order_id": str(orders[i] or ""),
                "variant_choice": str(variants[i] or ""),
            })
        result = batch_manager.save_variation_choices_bulk(choices)
        if result.get("ok") and int(result.get("saved", 0)) >= 0:
            saved = int(result.get("saved", 0))
            errs = result.get("errors", []) or []
            parts = [_ui("Saved {saved} variation choice(s)", saved=saved)]
            if errs:
                parts.append(_ui("Skipped: {count} invalid choice(s)", count=len(errs)))
            return _redirect_with_message("/unprocessed", _join_ui_parts(parts, sep=". "))
        return _redirect_ui("/unprocessed", "Could not save variation choices")

    @app.post("/resolve/generate-selected-variations")
    def resolve_generate_selected_variations():
        result = batch_manager.resolve_selected_variations()
        generated = int(result.get("generated", 0))
        remaining = int(result.get("remaining", 0))
        errors = result.get("errors", []) or []
        parts = [_ui("Generated {generated} variation output(s). Remaining variation queue: {remaining}", generated=generated, remaining=remaining)]
        combined = result.get("combined", {})
        if isinstance(combined, dict) and combined.get("ok"):
            parts.append(_ui("Combined PDF ready ({count} files)", count=combined.get("count", 0)))
        if errors:
            parts.append(_ui("Errors: {count}", count=len(errors)))
        target = "/unprocessed"
        if generated > 0 and remaining == 0:
            parts.append(_ui("All unprocessed labels are back in the latest batch. Open Combined PDF from the dashboard."))
            target = "/"
        return _redirect_with_message(target, _join_ui_parts(parts, sep=". "))

    @app.post("/resolve/save-process-all-variations")
    async def resolve_save_process_all_variations(request: Request):
        form = await request.form()
        labels = list(form.getlist("label_pdf"))
        orders = list(form.getlist("order_id"))
        variants = list(form.getlist("variant_choice"))
        choices: list[dict[str, str]] = []
        for i in range(min(len(labels), len(orders), len(variants))):
            choice = str(variants[i] or "").strip()
            if not choice:
                continue
            choices.append({
                "label_pdf": str(labels[i] or ""),
                "order_id": str(orders[i] or ""),
                "variant_choice": choice,
            })
        if not choices:
            return _redirect_ui("/unprocessed", "Choose at least one variation first")
        saved_result = batch_manager.save_variation_choices_bulk(choices)
        if not saved_result.get("ok"):
            return _redirect_ui("/unprocessed", "Could not save variation choices")
        process_result = batch_manager.resolve_selected_variations()
        saved = int(saved_result.get("saved", 0))
        generated = int(process_result.get("generated", 0))
        remaining = int(process_result.get("remaining", 0))
        errors = list(saved_result.get("errors", []) or []) + list(process_result.get("errors", []) or [])
        parts = [
            _ui("Saved {saved} variation choice(s)", saved=saved),
            _ui("Generated {generated} variation output(s). Remaining variation queue: {remaining}", generated=generated, remaining=remaining),
        ]
        combined = process_result.get("combined", {})
        if isinstance(combined, dict) and combined.get("ok"):
            parts.append(_ui("Combined PDF ready ({count} files)", count=combined.get("count", 0)))
        if errors:
            parts.append(_ui("Errors: {count}", count=len(errors)))
        target = "/unprocessed"
        if generated > 0 and remaining == 0:
            parts.append(_ui("All unprocessed labels are back in the latest batch. Open Combined PDF from the dashboard."))
            target = "/"
        return _redirect_with_message(target, _join_ui_parts(parts, sep=". "))

    @app.post("/resolve/remove")
    async def resolve_remove(label_pdf: str = Form(...)):
        ok = batch_manager.remove_unresolved_entry(label_pdf)
        if ok:
            return _redirect_ui("/unprocessed", "Queue item removed")
        return _redirect_ui("/unprocessed", "Queue item not found")

    @app.post("/resolve/assign")
    async def resolve_assign(label_pdf: str = Form(...), order_id: str = Form(...), variant_choice: str = Form("")):
        result = batch_manager.resolve_unmatched(label_pdf, order_id, variant_choice)
        if result.get("ok"):
            parts = [_ui("Resolved and output generated")]
            combined = result.get("combined", {})
            if isinstance(combined, dict) and combined.get("ok"):
                parts.append(_ui("Combined PDF ready ({count} files)", count=combined.get("count", 0)))
            target = "/unprocessed"
            if int(result.get("remaining", 0) or 0) == 0:
                parts.append(_ui("All unprocessed labels are back in the latest batch. Open Combined PDF from the dashboard."))
                target = "/"
            return _redirect_with_message(target, _join_ui_parts(parts, sep=". "))
        return _redirect_with_message("/unprocessed", str(result.get("error", _ui("Resolve failed"))))

    return app


app = create_app()


























