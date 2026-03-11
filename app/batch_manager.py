from __future__ import annotations

import copy
import json
import logging
import re
import shutil
import zipfile

import fitz
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter

from .item_db import ItemDB
from .label_text_extractor import extract_label_signals
from .label_matcher import match_label
from .order_parser import parse_amazon_packing_slips, parse_amazon_tsv, parse_ebay_csv
from .overlay_renderer import build_overlay_lines, build_compact_overlay_lines, create_backside_pdf, create_info_panel_overlay_pdf, create_overlay_pdf, create_summary_half_page, get_page_size
from .pdf_merge import append_backside_page, merge_overlay_on_first_page, merge_overlays_on_first_page
from .platform_detector import detect_platform_from_path, parse_order_id_from_filename
from .settings_manager import SettingsManager
from .utils import atomic_write_json, sanitize_filename


ORDER_ID_RE = re.compile(r"\d{3}-\d{7}-\d{7}")

def _natural_text_key(value: str) -> list[Any]:
    parts = re.split(r"(\d+)", str(value or "").replace("\\", "/"))
    out: list[Any] = []
    for part in parts:
        if not part:
            continue
        out.append(int(part) if part.isdigit() else part.lower())
    return out

def _path_sort_key(path: Path) -> list[Any]:
    return _natural_text_key(str(path))


def _norm_tracking_value(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def _norm_tracking_value(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())



class BatchManager:
    def __init__(self, settings: SettingsManager) -> None:
        self.settings = settings
        self.item_db = ItemDB(settings.items_csv_path, settings.config.get("new_item_defaults", {}))
        self.unresolved_queue_path = settings.processed_root_folder / "unresolved_queue.json"

    def _batch_resolution_overrides_path(self, batch_dir: Path | None) -> Path | None:
        if batch_dir is None:
            return None
        return batch_dir / "resolution_overrides.json"

    def _load_resolution_overrides(self, batch_dir: Path | None) -> list[dict[str, Any]]:
        path = self._batch_resolution_overrides_path(batch_dir)
        if path is None or not path.exists():
            return []
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return rows if isinstance(rows, list) else []

    def _save_resolution_override(self, batch_dir: Path | None, queue_entry: dict[str, Any], order: dict[str, Any]) -> None:
        path = self._batch_resolution_overrides_path(batch_dir)
        if path is None:
            return
        rows = self._load_resolution_overrides(batch_dir)
        label_pdf = str(queue_entry.get("label_pdf", "") or "").strip()
        label_name = Path(label_pdf).name if label_pdf else ""
        payload = {
            "label_pdf": label_pdf,
            "label_name": label_name,
            "order_id": str(order.get("order_id", "") or "").strip(),
            "order": copy.deepcopy(order),
        }
        kept = [r for r in rows if str(r.get("label_name", "") or "") != label_name]
        kept.append(payload)
        atomic_write_json(path, kept)

    def _find_latest_source_pdf_for_label_name(self, label_name: str) -> Path | None:
        if not label_name:
            return None
        queue = self._load_unresolved_queue()
        for row in queue:
            current = Path(str(row.get("label_pdf", "") or "")).name
            if current != label_name:
                continue
            src = self._resolve_source_pdf_from_queue_entry(row)
            if src is not None and src.exists():
                return src
        split_dir = self._split_runtime_dir()
        candidate = split_dir / label_name
        if candidate.exists():
            return candidate
        return None

    def _reapply_resolution_overrides(self, source_batch_dir: Path | None) -> dict[str, Any]:
        rows = self._load_resolution_overrides(source_batch_dir)
        if not rows:
            return {"applied": 0, "remaining": 0}

        latest = self._latest_batch_dir()
        if latest is None:
            return {"applied": 0, "remaining": len(rows)}

        idx = self.item_db.index()
        queue = self._load_unresolved_queue()
        applied = 0
        keep_overrides: list[dict[str, Any]] = []

        for row in rows:
            label_name = str(row.get("label_name", "") or "").strip()
            order = row.get("order") if isinstance(row.get("order"), dict) else None
            if not label_name or order is None:
                continue
            target = None
            for entry in queue:
                if Path(str(entry.get("label_pdf", "") or "")).name == label_name:
                    target = entry
                    break
            if target is None:
                keep_overrides.append(row)
                continue
            source_pdf = self._resolve_source_pdf_from_queue_entry(target)
            if source_pdf is None or not source_pdf.exists():
                source_pdf = self._find_latest_source_pdf_for_label_name(label_name)
            if source_pdf is None or not source_pdf.exists():
                keep_overrides.append(row)
                continue
            try:
                self.write_resolved_label_into_latest_batch(target, order, source_pdf, idx)
                queue = [q for q in queue if Path(str(q.get("label_pdf", "") or "")).name != label_name]
                applied += 1
            except Exception:
                logging.exception("Failed to reapply saved queue resolution for %s", label_name)
                keep_overrides.append(row)

        self._save_unresolved_queue(queue)
        target_path = self._batch_resolution_overrides_path(latest)
        if target_path is not None:
            atomic_write_json(target_path, rows)
        return {"applied": applied, "remaining": len(keep_overrides)}


    def _sum_order_quantity(self, order: dict[str, Any]) -> int:
        total = 0
        for item in order.get("items", []) or []:
            try:
                total += int(item.get("quantity", 1) or 1)
            except Exception:
                total += 1
        return total

    def _merged_order_from_queue_candidates(self, target: dict[str, Any], order_ids: list[str]) -> dict[str, Any] | None:
        wanted: list[str] = []
        for order_id in order_ids:
            oid = str(order_id or "").strip()
            if oid and oid not in wanted:
                wanted.append(oid)
        if len(wanted) < 2:
            return None

        candidates_by_id: dict[str, dict[str, Any]] = {}
        for candidate in target.get("candidates", []) or []:
            oid = str(candidate.get("order_id", "") or "").strip()
            order = candidate.get("order") if isinstance(candidate.get("order"), dict) else None
            if oid and order is not None:
                candidates_by_id[oid] = order

        selected_orders: list[dict[str, Any]] = []
        for oid in wanted:
            order = candidates_by_id.get(oid)
            if order is None:
                return None
            if str(order.get("platform", "") or "").strip().lower() != "ebay":
                return None
            selected_orders.append(copy.deepcopy(order))

        if len(selected_orders) < 2:
            return None

        tracking_values = {_norm_tracking_value(order.get("tracking_number", "") or "") for order in selected_orders if _norm_tracking_value(order.get("tracking_number", "") or "")}
        if len(tracking_values) > 1:
            return None

        primary = selected_orders[0]
        merged = copy.deepcopy(primary)
        merged_items: list[dict[str, Any]] = []
        total_paid = 0.0
        subtotal_paid = 0.0
        item_subtotal_paid = 0.0
        shipping_subtotal_paid = 0.0
        sale_date = str(primary.get("sale_date", "") or "")
        sale_date_sort = str(primary.get("sale_date_sort", "") or "")

        for order in selected_orders:
            merged_items.extend(copy.deepcopy(order.get("items", []) or []))
            try:
                total_paid += float(order.get("total_paid", 0.0) or 0.0)
            except Exception:
                pass
            try:
                subtotal_paid += float(order.get("subtotal_paid", 0.0) or 0.0)
            except Exception:
                pass
            try:
                item_subtotal_paid += float(order.get("item_subtotal_paid", 0.0) or 0.0)
            except Exception:
                pass
            try:
                shipping_subtotal_paid += float(order.get("shipping_subtotal_paid", 0.0) or 0.0)
            except Exception:
                pass
            cur_sort = str(order.get("sale_date_sort", "") or "")
            if cur_sort and cur_sort > sale_date_sort:
                sale_date_sort = cur_sort
                sale_date = str(order.get("sale_date", "") or sale_date)

        merged["items"] = merged_items
        merged["total_paid"] = round(total_paid, 2)
        merged["subtotal_paid"] = round(subtotal_paid, 2)
        merged["item_subtotal_paid"] = round(item_subtotal_paid, 2)
        merged["shipping_subtotal_paid"] = round(shipping_subtotal_paid, 2)
        merged["merged_order_ids"] = wanted
        merged["merged_order_count"] = len(wanted)
        merged["sale_date"] = sale_date
        merged["sale_date_sort"] = sale_date_sort
        return merged
    def scan_inputs(self) -> dict[str, Any]:
        files = self._all_batch_files()
        labels = self._find_label_pdfs(files)
        ebay_csv = self._find_ebay_csv(files)
        amazon_txt = self._find_amazon_txt(files)
        unresolved = self._load_unresolved_queue()

        zip_count = len([p for p in files if p.suffix.lower() == ".zip"])
        pdf_count = len([p for p in files if p.suffix.lower() == ".pdf"])
        csv_count = len([p for p in files if p.suffix.lower() == ".csv"])
        txt_count = len([p for p in files if p.suffix.lower() == ".txt"])

        return {
            "label_count": len(labels),
            "label_source_count": len(labels) + zip_count,
            "staged_file_count": len(files),
            "zip_count": zip_count,
            "pdf_count": pdf_count,
            "csv_count": csv_count,
            "txt_count": txt_count,
            "ebay_csv_found": ebay_csv is not None,
            "amazon_txt_found": amazon_txt is not None,
            "unresolved_count": len(unresolved),
            "files": [str(p) for p in files],
            "staged_file_names": [self._describe_staged_file(p) for p in files],
        }

    def _all_batch_files(self) -> list[Path]:
        root = self.settings.incoming_batch_folder
        root.mkdir(parents=True, exist_ok=True)
        files = sorted([p for p in root.rglob("*") if p.is_file()], key=_path_sort_key)
        return [p for p in files if "_split_pages" not in p.parts]

    def _extract_zip_files(self) -> list[Path]:
        extracted: list[Path] = []
        root = self.settings.incoming_batch_folder
        for z in sorted(root.glob("*.zip"), key=_path_sort_key):
            dest = root / "_unzipped" / z.stem
            dest.mkdir(parents=True, exist_ok=True)
            try:
                with zipfile.ZipFile(z, "r") as zf:
                    zf.extractall(dest)
                extracted.extend([p for p in dest.rglob("*.pdf")])
            except Exception:
                logging.exception("Failed to extract ZIP: %s", z)
        return extracted

    def _find_label_pdfs(self, files: list[Path]) -> list[Path]:
        out: list[Path] = []
        for p in files:
            if p.suffix.lower() != ".pdf":
                continue
            if "packing slip" in p.name.lower():
                continue
            out.append(p)
        return out


    def _split_runtime_dir(self) -> Path:
        d = self.settings.incoming_batch_folder / "_split_pages"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _expand_multi_page_label_pdfs(self, label_pdfs: list[Path]) -> list[Path]:
        out: list[Path] = []
        split_dir = self._split_runtime_dir()

        # Fresh split output each batch run to avoid stale page files.
        shutil.rmtree(split_dir, ignore_errors=True)
        split_dir.mkdir(parents=True, exist_ok=True)

        for src in label_pdfs:
            try:
                reader = PdfReader(str(src))
            except Exception:
                logging.exception("Failed to open PDF while checking page count: %s", src)
                out.append(src)
                continue

            pages = len(reader.pages)
            if pages <= 1:
                out.append(src)
                continue

            base = sanitize_filename(src.stem) or "label"
            for i, page in enumerate(reader.pages, start=1):
                one = split_dir / f"{base}__p{i:03d}.pdf"
                try:
                    writer = PdfWriter()
                    writer.add_page(page)
                    with one.open("wb") as f:
                        writer.write(f)
                    out.append(one)
                except Exception:
                    logging.exception("Failed to split page %s from %s", i, src)
        return out

    def _find_packing_slips(self, files: list[Path]) -> list[Path]:
        out: list[Path] = []
        for p in files:
            if p.suffix.lower() != ".pdf":
                continue
            name = p.name.lower()
            if "packing slip" in name or self._pdf_looks_like_packing_slip(p):
                out.append(p)
        return out
    def _pdf_looks_like_packing_slip(self, path: Path) -> bool:
        try:
            reader = PdfReader(str(path))
            text = ((reader.pages[0].extract_text() or "") if reader.pages else "").lower()
        except Exception:
            return False
        if not text:
            return False
        if "packing slip" in text:
            return True
        has_order = bool(ORDER_ID_RE.search(text)) or ("order id" in text)
        has_item_markers = (" asin" in f" {text}") or (" sku" in f" {text}")
        has_shipto = "ship to" in text or "recipient" in text
        return has_order and has_item_markers and has_shipto

    def _find_ebay_csvs(self, files: list[Path]) -> list[Path]:
        out = [p for p in files if p.suffix.lower() == ".csv" and "ordersreport" in p.name.lower()]
        out.sort(key=lambda p: str(p).lower())
        return out

    def _find_ebay_csv(self, files: list[Path]) -> Path | None:
        rows = self._find_ebay_csvs(files)
        return rows[0] if rows else None

    def _looks_like_amazon_tsv(self, path: Path) -> bool:
        try:
            first = path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()[0].lower()
        except Exception:
            return False
        required = ["order-id", "sku", "quantity-purchased"]
        return all(r in first for r in required)

    def _find_amazon_txts(self, files: list[Path]) -> list[Path]:
        txts = [p for p in files if p.suffix.lower() == ".txt"]
        primary = [p for p in txts if self._looks_like_amazon_tsv(p)]
        if primary:
            return sorted(primary, key=lambda p: str(p).lower())
        fallback = [p for p in txts if "order report" in p.name.lower()]
        return sorted(fallback, key=lambda p: str(p).lower())

    def _find_amazon_txt(self, files: list[Path]) -> Path | None:
        rows = self._find_amazon_txts(files)
        return rows[0] if rows else None

    def _merge_order_record(self, orders: dict[str, dict[str, Any]], incoming: dict[str, Any]) -> None:
        order_id = str(incoming.get("order_id", "") or "").strip()
        if not order_id:
            return
        existing = orders.get(order_id)
        if existing is None:
            orders[order_id] = incoming
            return

        for k in ("ship_name", "ship_postal", "tracking_number", "platform"):
            if not existing.get(k) and incoming.get(k):
                existing[k] = incoming.get(k)

        seen: set[tuple[str, str, str, int, int]] = set()
        merged_items: list[dict[str, Any]] = []
        for src in (existing.get("items", []) or []):
            sig = (
                str(src.get("item_id", "") or "").strip(),
                str(src.get("item_sku", "") or "").strip(),
                str(src.get("item_asin", "") or "").strip().upper(),
                int(src.get("quantity", 1) or 1),
                int(round(float(src.get("line_total", 0.0) or 0.0) * 100)),
            )
            if sig in seen:
                continue
            seen.add(sig)
            merged_items.append(src)
        for src in (incoming.get("items", []) or []):
            sig = (
                str(src.get("item_id", "") or "").strip(),
                str(src.get("item_sku", "") or "").strip(),
                str(src.get("item_asin", "") or "").strip().upper(),
                int(src.get("quantity", 1) or 1),
                int(round(float(src.get("line_total", 0.0) or 0.0) * 100)),
            )
            if sig in seen:
                continue
            seen.add(sig)
            merged_items.append(src)
        existing["items"] = merged_items

        total = 0.0
        for itm in merged_items:
            try:
                total += float(itm.get("line_total", 0.0) or 0.0)
            except Exception:
                pass
        if total > 0:
            existing["total_paid"] = total
        elif not existing.get("total_paid") and incoming.get("total_paid"):
            existing["total_paid"] = incoming.get("total_paid")

        src_existing = str(existing.get("source", "") or "").strip()
        src_incoming = str(incoming.get("source", "") or "").strip()
        if not src_existing and src_incoming:
            existing["source"] = src_incoming

    def _describe_staged_file(self, path: Path) -> str:
        name = path.name
        lower = name.lower()
        kind = path.suffix.lower().lstrip('.') or 'file'
        if path.suffix.lower() == '.txt' and self._looks_like_amazon_tsv(path):
            kind = 'Amazon TXT'
        elif path.suffix.lower() == '.csv' and 'ordersreport' in lower:
            kind = 'eBay CSV'
        elif path.suffix.lower() == '.zip':
            kind = 'Amazon ZIP' if ('amzn' in lower or 'amazon' in lower) else 'ZIP'
        elif path.suffix.lower() == '.pdf':
            plat = detect_platform_from_path(path)
            kind = f"{plat.title()} PDF" if plat in ('amazon', 'ebay') else 'PDF'
        return f"[{kind}] {name}"

    def _extract_amazon_order_ids_from_labels(self, label_pdfs: list[Path]) -> set[str]:
        ids: set[str] = set()
        for p in label_pdfs:
            m = ORDER_ID_RE.search(p.name)
            if m:
                ids.add(m.group(0))
        return ids

    def _should_filter_amazon_report_by_label_ids(self, amazon_label_pdfs: list[Path], amazon_ids: set[str]) -> bool:
        if not amazon_label_pdfs or not amazon_ids:
            return False
        # Only apply strict overlap filtering when filename order IDs cover most Amazon labels.
        coverage = len(amazon_ids) / max(1, len(amazon_label_pdfs))
        return coverage >= 0.7
    def _build_orders(self, files: list[Path], label_pdfs: list[Path]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        ebay_csvs = self._find_ebay_csvs(files)
        amazon_txts = self._find_amazon_txts(files)
        packing_slips = self._find_packing_slips(files)

        orders: dict[str, dict[str, Any]] = {}
        warnings: dict[str, Any] = {"scientific_notation_item_numbers": 0}
        for ebay_csv in ebay_csvs:
            parsed_ebay, ebay_warnings = parse_ebay_csv(ebay_csv)
            warnings["scientific_notation_item_numbers"] += int(ebay_warnings.get("scientific_notation_item_numbers", 0) or 0)
            for rec in parsed_ebay.values():
                self._merge_order_record(orders, rec)

        amazon_label_pdfs = [p for p in label_pdfs if detect_platform_from_path(p) == "amazon"]
        amazon_ids = self._extract_amazon_order_ids_from_labels(amazon_label_pdfs)
        filter_by_ids = self._should_filter_amazon_report_by_label_ids(amazon_label_pdfs, amazon_ids)
        for amazon_txt in amazon_txts:
            parsed = parse_amazon_tsv(amazon_txt, allowed_order_ids=amazon_ids if filter_by_ids else None)
            for rec in parsed.values():
                rec["source"] = "amazon_report"
                self._merge_order_record(orders, rec)

        if packing_slips:
            slip_rows = parse_amazon_packing_slips(packing_slips, allowed_order_ids=amazon_ids if filter_by_ids else None)
            if amazon_txts:
                self._enrich_amazon_orders_with_packing_slips(orders, slip_rows)
            else:
                for rec in slip_rows.values():
                    rec["source"] = "packing_slip"
                    self._merge_order_record(orders, rec)

        for order_id in amazon_ids:
            if order_id not in orders:
                orders[order_id] = {
                    "platform": "amazon",
                    "order_id": order_id,
                    "ship_name": "",
                    "ship_postal": "",
                    "tracking_number": "",
                    "items": [{"item_id": "UNKNOWN", "title": "Unknown Amazon Item", "quantity": 1, "line_total": 0.0}],
                    "total_paid": 0.0,
                    "source": "stub",
                }
        if not warnings.get("scientific_notation_item_numbers"):
            warnings = {}
        return orders, warnings

    def _enrich_amazon_orders_with_packing_slips(self, orders: dict[str, dict[str, Any]], slip_rows: dict[str, dict[str, Any]]) -> None:
        for order_id, slip in slip_rows.items():
            rec = orders.get(order_id)
            if not rec or str(rec.get("platform", "")).lower() != "amazon":
                continue

            if not rec.get("ship_postal") and slip.get("ship_postal"):
                rec["ship_postal"] = slip.get("ship_postal", "")

            slip_items = slip.get("items", []) or []
            report_items = rec.get("items", []) or []
            if not slip_items or not report_items:
                continue

            # 1) SKU-first enrichment: add ASIN/title from slip row onto matching TXT SKU row.
            slip_by_sku: dict[str, dict[str, Any]] = {}
            for s in slip_items:
                sku = str(s.get("item_sku", "")).strip()
                if sku:
                    slip_by_sku[sku] = s

            used_slip_ids: set[int] = set()
            for r in report_items:
                r_sku = str(r.get("item_sku", "") or r.get("item_id", "")).strip()
                if not r_sku:
                    continue
                s = slip_by_sku.get(r_sku)
                if not s:
                    continue

                s_asin = str(s.get("item_asin", "") or s.get("item_id", "")).strip().upper()
                if s_asin and s_asin != "UNKNOWN" and not str(r.get("item_asin", "")).strip():
                    r["item_asin"] = s_asin

                if not str(r.get("title", "")).strip() or str(r.get("title", "")).strip().lower() == "unknown amazon item":
                    s_title = str(s.get("title", "")).strip()
                    if s_title:
                        r["title"] = s_title

                used_slip_ids.add(id(s))

            # 2) Fallback by position for any remaining rows.
            fallback = [s for s in slip_items if id(s) not in used_slip_ids]
            for i, r in enumerate(report_items):
                if str(r.get("item_asin", "")).strip():
                    continue
                if i >= len(fallback):
                    break
                s = fallback[i]
                s_asin = str(s.get("item_asin", "") or s.get("item_id", "")).strip().upper()
                if s_asin and s_asin != "UNKNOWN":
                    r["item_asin"] = s_asin

                if not str(r.get("title", "")).strip() or str(r.get("title", "")).strip().lower() == "unknown amazon item":
                    s_title = str(s.get("title", "")).strip()
                    if s_title:
                        r["title"] = s_title
    def _sync_packing_slips_to_item_db(self, files: list[Path]) -> dict[str, int]:
        slips = self._find_packing_slips(files)
        if not slips:
            return {"processed_slips": 0, "items_touched": 0}

        parsed = parse_amazon_packing_slips(slips, allowed_order_ids=None)
        idx = self.item_db.index()
        touched = 0

        for order in parsed.values():
            for item in order.get("items", []) or []:
                sku = str(item.get("item_sku", "")).strip()
                asin = str(item.get("item_asin", "")).strip().upper()
                iid = str(item.get("item_id", "")).strip()
                title = str(item.get("title", "")).strip()

                primary = sku or iid or asin
                if not primary or primary.upper() == "UNKNOWN":
                    continue

                existing = None
                for k in [primary, sku, asin, iid]:
                    k = (k or "").strip()
                    if not k:
                        continue
                    existing = idx.get(("amazon", k))
                    if existing is not None:
                        break

                row = self.item_db.ensure_item("amazon", primary, title=title, item_asin=asin)
                for alias in [row.get("item_id", ""), row.get("amazon_sku", ""), row.get("amazon_asin", "")]:
                    alias = (alias or "").strip()
                    if alias:
                        idx[("amazon", alias)] = row
                if existing is None:
                    touched += 1

        return {"processed_slips": len(slips), "items_touched": touched}
    def _label_platform_partition(self, label_pdf: Path, signals: dict[str, Any]) -> str:
        sig = str(signals.get("platform_hint", "") or "").strip().lower()
        if sig in ("amazon", "ebay"):
            return sig
        name_hint = str(detect_platform_from_path(label_pdf) or "").strip().lower()
        if name_hint in ("amazon", "ebay"):
            return name_hint
        return "unknown"

    def _partition_orders_by_platform(self, orders: dict[str, dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
        out: dict[str, dict[str, dict[str, Any]]] = {"amazon": {}, "ebay": {}, "unknown": {}}
        for order_id, order in orders.items():
            platform = str(order.get("platform", "") or "").strip().lower()
            if platform not in ("amazon", "ebay"):
                platform = "unknown"
            out[platform][order_id] = order
        return out

    def _compatible_orders_for_label(
        self,
        label_platform: str,
        orders_partition: dict[str, dict[str, dict[str, Any]]],
        all_orders: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], str]:
        lp = str(label_platform or "").strip().lower()
        amz = orders_partition.get("amazon", {}) or {}
        ebay = orders_partition.get("ebay", {}) or {}
        unknown = orders_partition.get("unknown", {}) or {}
        if lp in ("amazon", "ebay"):
            return (orders_partition.get(lp, {}) or {}), lp
        if amz and not ebay:
            return amz, "amazon"
        if ebay and not amz:
            return ebay, "ebay"
        if unknown and not amz and not ebay:
            return unknown, ""
        # Mixed-platform staged batch + unknown label platform: do not cross-match.
        return {}, ""
    def _build_preflight_partition(
        self,
        label_pdfs: list[Path],
        orders: dict[str, dict[str, Any]],
        signals_for: Any,
    ) -> dict[str, Any]:
        label_platforms: dict[str, str] = {}
        label_counts = {"amazon": 0, "ebay": 0, "unknown": 0}
        for label_pdf in label_pdfs:
            platform = self._label_platform_partition(label_pdf, signals_for(label_pdf))
            label_platforms[str(label_pdf)] = platform
            label_counts[platform] = label_counts.get(platform, 0) + 1
        orders_partition = self._partition_orders_by_platform(orders)
        order_counts = {
            "amazon": len(orders_partition.get("amazon", {}) or {}),
            "ebay": len(orders_partition.get("ebay", {}) or {}),
            "unknown": len(orders_partition.get("unknown", {}) or {}),
        }
        return {
            "label_platforms": label_platforms,
            "label_counts": label_counts,
            "orders_partition": orders_partition,
            "order_counts": order_counts,
        }

    def process_batch(self) -> dict[str, Any]:
        logging.info("Batch start")
        self._extract_zip_files()
        files = self._all_batch_files()
        label_pdfs = self._find_label_pdfs(files)
        label_pdfs = self._expand_multi_page_label_pdfs(label_pdfs)
        if not label_pdfs:
            sync = self._sync_packing_slips_to_item_db(files)
            if sync.get("processed_slips", 0) <= 0:
                return {"ok": False, "error": "No label PDFs found in incoming/batch."}

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_dir = self.settings.processed_root_folder / f"batch_{ts}"
            archive_dir = batch_dir / "input_archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            report: dict[str, Any] = {
                "timestamp": ts,
                "mode": "packing_slip_sync",
                "results": [],
                "summary": {
                    "matched": 0,
                    "unresolved": 0,
                    "errors": 0,
                    "synced_items": int(sync.get("items_touched", 0)),
                    "processed_slips": int(sync.get("processed_slips", 0)),
                },
            }

            atomic_write_json(batch_dir / "batch_report.json", report)
            self._archive_inputs(files, archive_dir)
            logging.info("Batch end (packing slip sync): %s", report["summary"])
            return {"ok": True, "batch_dir": str(batch_dir), "report": report}

        orders, build_warnings = self._build_orders(files, label_pdfs)
        if not orders:
            return {"ok": False, "error": "No order data found. Add eBay OrdersReport CSV and/or Amazon Order Report TXT."}

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_dir = self.settings.processed_root_folder / f"batch_{ts}"
        output_dir = batch_dir / "output_pdfs"
        archive_dir = batch_dir / "input_archive"
        output_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        unresolved_queue = self._load_unresolved_queue()
        signals_cache: dict[str, dict[str, Any]] = {}
        def _signals_for(path: Path) -> dict[str, Any]:
            key = str(path)
            if key not in signals_cache:
                signals_cache[key] = extract_label_signals(path)
            return signals_cache[key]
        preflight = self._build_preflight_partition(label_pdfs, orders, _signals_for)
        report: dict[str, Any] = {
            "timestamp": ts,
            "results": [],
            "warnings": build_warnings,
            "summary": {
                "matched": 0,
                "unresolved": 0,
                "errors": 0,
                "preflight": {
                    "labels": preflight.get("label_counts", {}),
                    "orders": preflight.get("order_counts", {}),
                },
            },
        }
        idx = self.item_db.index()

        for label_pdf in label_pdfs:
            try:
                unresolved_queue = [q for q in unresolved_queue if str(q.get("label_pdf", "")) != str(label_pdf)]
                label_signals = _signals_for(label_pdf)
                platform = str(preflight.get("label_platforms", {}).get(str(label_pdf), "unknown") or "unknown")
                compatible_orders, match_hint = self._compatible_orders_for_label(platform, preflight.get("orders_partition", {}), orders)
                if not compatible_orders:
                    unresolved = {
                        "label_pdf": str(label_pdf),
                        "label_identity": self._build_label_identity(label_pdf, label_signals),
                        "recipient_name": label_signals.get("recipient_name", ""),
                        "tracking_number": label_signals.get("tracking_number", ""),
                        "ship_postal": label_signals.get("ship_postal", ""),
                        "reason": "no_compatible_order_source",
                        "candidates": [],
                    }
                    unresolved_queue.append(unresolved)
                    report["results"].append({"label_pdf": str(label_pdf), "status": "unresolved", "reason": unresolved["reason"]})
                    report["summary"]["unresolved"] += 1
                    continue

                direct_order = None
                if (match_hint or platform) == "amazon":
                    file_order_id = parse_order_id_from_filename(label_pdf)
                    if file_order_id and file_order_id in compatible_orders:
                        direct_order = compatible_orders[file_order_id]

                if direct_order is not None:
                    m = {
                        "status": "matched",
                        "method": "filename_order_id_direct",
                        "confidence": 1.0,
                        "order": direct_order,
                        "candidates": [],
                    }
                else:
                    exact_tracking_orders: list[dict[str, Any]] = []
                    exact_tracking = _norm_tracking_value(label_signals.get("tracking_number", "") or "")
                    if str(match_hint or platform).strip().lower() == "ebay" and exact_tracking:
                        for order in compatible_orders.values():
                            if _norm_tracking_value(order.get("tracking_number", "") or "") == exact_tracking:
                                exact_tracking_orders.append(copy.deepcopy(order))
                    if len(exact_tracking_orders) >= 2:
                        exact_tracking_orders.sort(key=lambda order: (str(order.get("sale_date_sort", "") or order.get("sale_date", "") or ""), str(order.get("order_id", "") or "")), reverse=True)
                        merged_order = copy.deepcopy(exact_tracking_orders[0])
                        merged_items: list[dict[str, Any]] = []
                        total_paid = 0.0
                        subtotal_paid = 0.0
                        item_subtotal_paid = 0.0
                        shipping_subtotal_paid = 0.0
                        merged_ids: list[str] = []
                        for order in exact_tracking_orders:
                            oid = str(order.get("order_id", "") or "").strip()
                            if oid and oid not in merged_ids:
                                merged_ids.append(oid)
                            merged_items.extend(copy.deepcopy(order.get("items", []) or []))
                            try:
                                total_paid += float(order.get("total_paid", 0.0) or 0.0)
                            except Exception:
                                pass
                            try:
                                subtotal_paid += float(order.get("subtotal_paid", 0.0) or 0.0)
                            except Exception:
                                pass
                            try:
                                item_subtotal_paid += float(order.get("item_subtotal_paid", 0.0) or 0.0)
                            except Exception:
                                pass
                            try:
                                shipping_subtotal_paid += float(order.get("shipping_subtotal_paid", 0.0) or 0.0)
                            except Exception:
                                pass
                        merged_order["items"] = merged_items
                        merged_order["total_paid"] = round(total_paid, 2)
                        merged_order["subtotal_paid"] = round(subtotal_paid, 2)
                        merged_order["item_subtotal_paid"] = round(item_subtotal_paid, 2)
                        merged_order["shipping_subtotal_paid"] = round(shipping_subtotal_paid, 2)
                        merged_order["merged_order_ids"] = merged_ids
                        merged_order["merged_order_count"] = len(merged_ids)
                        m = {
                            "status": "matched",
                            "method": "exact_tracking_multi_order",
                            "confidence": 1.25,
                            "order": merged_order,
                            "candidates": [],
                        }
                    else:
                        m = match_label(label_pdf, compatible_orders, platform_hint=match_hint if match_hint else "")
                        if m.get("status") != "matched" and str(match_hint or platform).strip().lower() == "ebay":
                            candidate_tracking_groups: dict[str, list[dict[str, Any]]] = {}
                            for candidate in m.get("candidates", []) or []:
                                candidate_order = candidate.get("order") if isinstance(candidate.get("order"), dict) else None
                                if candidate_order is None:
                                    continue
                                candidate_tracking = _norm_tracking_value(candidate_order.get("tracking_number", "") or candidate.get("tracking_number", "") or "")
                                if not candidate_tracking:
                                    continue
                                candidate_tracking_groups.setdefault(candidate_tracking, []).append(candidate)
                            eligible_groups = [group for group in candidate_tracking_groups.values() if len(group) >= 2]
                            if eligible_groups:
                                eligible_groups.sort(key=lambda group: (len(group), max(float(c.get("score", 0.0) or 0.0) for c in group)), reverse=True)
                                best_group = eligible_groups[0]
                                best_group.sort(key=lambda c: (str((c.get("order") or {}).get("sale_date_sort", "") or (c.get("order") or {}).get("sale_date", "") or ""), str(c.get("order_id", "") or "")), reverse=True)
                                merged_order = copy.deepcopy(best_group[0]["order"])
                                merged_items: list[dict[str, Any]] = []
                                total_paid = 0.0
                                subtotal_paid = 0.0
                                item_subtotal_paid = 0.0
                                shipping_subtotal_paid = 0.0
                                merged_ids: list[str] = []
                                for candidate in best_group:
                                    candidate_order = copy.deepcopy(candidate.get("order") or {})
                                    oid = str(candidate_order.get("order_id", "") or candidate.get("order_id", "") or "").strip()
                                    if oid and oid not in merged_ids:
                                        merged_ids.append(oid)
                                    merged_items.extend(copy.deepcopy(candidate_order.get("items", []) or []))
                                    try:
                                        total_paid += float(candidate_order.get("total_paid", 0.0) or 0.0)
                                    except Exception:
                                        pass
                                    try:
                                        subtotal_paid += float(candidate_order.get("subtotal_paid", 0.0) or 0.0)
                                    except Exception:
                                        pass
                                    try:
                                        item_subtotal_paid += float(candidate_order.get("item_subtotal_paid", 0.0) or 0.0)
                                    except Exception:
                                        pass
                                    try:
                                        shipping_subtotal_paid += float(candidate_order.get("shipping_subtotal_paid", 0.0) or 0.0)
                                    except Exception:
                                        pass
                                merged_order["items"] = merged_items
                                merged_order["total_paid"] = round(total_paid, 2)
                                merged_order["subtotal_paid"] = round(subtotal_paid, 2)
                                merged_order["item_subtotal_paid"] = round(item_subtotal_paid, 2)
                                merged_order["shipping_subtotal_paid"] = round(shipping_subtotal_paid, 2)
                                merged_order["merged_order_ids"] = merged_ids
                                merged_order["merged_order_count"] = len(merged_ids)
                                m = {
                                    "status": "matched",
                                    "method": "candidate_tracking_multi_order",
                                    "confidence": 1.1,
                                    "order": merged_order,
                                    "candidates": [],
                                }

                if m["status"] != "matched":
                    label_signals = _signals_for(label_pdf)
                    unresolved = {
                        "label_pdf": str(label_pdf),
                        "label_identity": self._build_label_identity(label_pdf, label_signals),
                        "recipient_name": label_signals.get("recipient_name", ""),
                        "tracking_number": label_signals.get("tracking_number", ""),
                        "ship_postal": label_signals.get("ship_postal", ""),
                        "reason": m.get("reason", "unresolved"),
                        "candidates": [
                            {
                                "order_id": c["order_id"],
                                "score": c["score"],
                                "ship_name": c["order"].get("ship_name", ""),
                                "ship_postal": c["order"].get("ship_postal", ""),
                                "order": c["order"],
                            }
                            for c in m.get("candidates", [])
                        ],
                    }
                    unresolved_queue.append(unresolved)
                    report["results"].append({"label_pdf": str(label_pdf), "status": "unresolved", "reason": unresolved["reason"]})
                    report["summary"]["unresolved"] += 1
                    continue

                order = m["order"]
                if order.get("platform") == "amazon" and order.get("source") == "stub":
                    label_signals = _signals_for(label_pdf)
                    unresolved = {
                        "label_pdf": str(label_pdf),
                        "label_identity": self._build_label_identity(label_pdf, label_signals),
                        "recipient_name": label_signals.get("recipient_name", ""),
                        "tracking_number": label_signals.get("tracking_number", ""),
                        "ship_postal": label_signals.get("ship_postal", ""),
                        "reason": "amazon_order_not_found_in_report",
                        "candidates": [],
                    }
                    unresolved_queue.append(unresolved)
                    report["results"].append({"label_pdf": str(label_pdf), "status": "unresolved", "reason": unresolved["reason"]})
                    report["summary"]["unresolved"] += 1
                    continue

                variation_options = self._order_variation_options(order, idx)
                if len(variation_options) >= 2:
                    label_signals = _signals_for(label_pdf)
                    unresolved = {
                        "label_pdf": str(label_pdf),
                        "label_identity": self._build_label_identity(label_pdf, label_signals),
                        "recipient_name": label_signals.get("recipient_name", ""),
                        "tracking_number": label_signals.get("tracking_number", ""),
                        "ship_postal": label_signals.get("ship_postal", ""),
                        "reason": "multi_variation_choice_required",
                        "variation_options": variation_options,
                        "order": order,
                        "selected_variation": "",
                        "selected_order_id": "",
                        "candidates": [
                            {
                                "order_id": order.get("order_id", ""),
                                "score": 1.0,
                                "ship_name": order.get("ship_name", ""),
                                "ship_postal": order.get("ship_postal", ""),
                                "order": order,
                            }
                        ],
                    }
                    unresolved_queue.append(unresolved)
                    report["results"].append({"label_pdf": str(label_pdf), "status": "unresolved", "reason": unresolved["reason"]})
                    report["summary"]["unresolved"] += 1
                    continue

                valid, validation_reason = self._validate_required_fields(order, idx)
                if not valid:
                    label_signals = _signals_for(label_pdf)
                    unresolved = {
                        "label_pdf": str(label_pdf),
                        "label_identity": self._build_label_identity(label_pdf, label_signals),
                        "recipient_name": label_signals.get("recipient_name", ""),
                        "tracking_number": label_signals.get("tracking_number", ""),
                        "ship_postal": label_signals.get("ship_postal", ""),
                        "reason": f"missing_required_fields:{validation_reason}",
                        "candidates": [
                            {
                                "order_id": order.get("order_id", ""),
                                "score": 1.0,
                                "ship_name": order.get("ship_name", ""),
                                "ship_postal": order.get("ship_postal", ""),
                                "order": order,
                            }
                        ],
                    }
                    unresolved_queue.append(unresolved)
                    report["results"].append({"label_pdf": str(label_pdf), "status": "unresolved", "reason": unresolved["reason"]})
                    report["summary"]["unresolved"] += 1
                    continue
                out_path = self._render_one_label(label_pdf, order, idx, output_dir)
                effective_carrier = self._effective_carrier(order, label_signals.get("carrier", ""))
                sort_meta = self._sort_meta_for_order(order, idx, effective_carrier)
                items = list(order.get("items", []) or [])
                total_paid = 0.0
                try:
                    total_paid = float(order.get("total_paid", 0.0) or 0.0)
                except Exception:
                    total_paid = 0.0
                quantity_total = 0
                item_keys: list[str] = []
                item_titles: list[str] = []
                for item in items:
                    try:
                        quantity_total += int(item.get("quantity", 1) or 1)
                    except Exception:
                        quantity_total += 1
                    key = str(
                        item.get("item_sku", "")
                        or item.get("ebay_item_number", "")
                        or item.get("item_id", "")
                        or item.get("item_asin", "")
                        or ""
                    ).strip()
                    if key and key not in item_keys:
                        item_keys.append(key)
                    title = str(item.get("title", "") or "").strip()
                    if title and title not in item_titles:
                        item_titles.append(title)
                report["results"].append(
                    {
                        "label_pdf": str(label_pdf),
                        "status": "matched",
                        "order_id": m["order"].get("order_id", ""),
                        "platform": m["order"].get("platform", ""),
                        "ship_name": m["order"].get("ship_name", ""),
                        "ship_postal": m["order"].get("ship_postal", ""),
                        "tracking_number": m["order"].get("tracking_number", ""),
                        "carrier": effective_carrier,
                        "method": m.get("method", ""),
                        "confidence": m.get("confidence", 0),
                        "output_pdf": str(out_path),
                        "process_index": len(report["results"]),
                        "sort_label": sort_meta.get("label", ""),
                        "sort_qty": sort_meta.get("qty", 0),
                        "sort_item_key": sort_meta.get("item_key", ""),
                        "sort_location": sort_meta.get("location", ""),
                        "sort_carrier": sort_meta.get("carrier", ""),
                        "item_count": len(items),
                        "quantity_total": quantity_total,
                        "total_paid": round(total_paid, 2),
                        "item_keys": item_keys,
                        "item_titles": item_titles,
                    }
                )
                report["summary"]["matched"] += 1
            except Exception as ex:
                logging.exception("Error processing %s", label_pdf)
                report["summary"]["errors"] += 1
                report["results"].append({"label_pdf": str(label_pdf), "status": "error", "error": str(ex)})

        atomic_write_json(batch_dir / "batch_report.json", report)
        self._save_unresolved_queue(unresolved_queue)
        self._archive_inputs(files, archive_dir)

        logging.info("Batch end: %s", report["summary"])
        return {"ok": True, "batch_dir": str(batch_dir), "report": report}

    def _recount_batch_summary(self, report: dict[str, Any]) -> None:
        results = report.get("results", []) if isinstance(report, dict) else []
        summary = dict(report.get("summary", {}) if isinstance(report, dict) else {})
        summary["matched"] = sum(1 for r in results if str(r.get("status", "")).lower() == "matched")
        summary["unresolved"] = sum(1 for r in results if str(r.get("status", "")).lower() == "unresolved")
        summary["errors"] = sum(1 for r in results if str(r.get("status", "")).lower() == "error")
        report["summary"] = summary

    def _build_matched_report_entry(
        self,
        *,
        label_pdf: str,
        source_pdf: Path,
        order: dict[str, Any],
        output_pdf: Path,
        idx: dict[tuple[str, str], dict[str, str]],
        process_index: int,
    ) -> dict[str, Any]:
        label_signals = extract_label_signals(source_pdf)
        effective_carrier = self._effective_carrier(order, label_signals.get("carrier", ""))
        sort_meta = self._sort_meta_for_order(order, idx, effective_carrier)
        items = list(order.get("items", []) or [])
        total_paid = 0.0
        try:
            total_paid = float(order.get("total_paid", 0.0) or 0.0)
        except Exception:
            total_paid = 0.0

        quantity_total = 0
        item_keys: list[str] = []
        item_titles: list[str] = []
        for item in items:
            try:
                quantity_total += int(item.get("quantity", 1) or 1)
            except Exception:
                quantity_total += 1

            key = str(
                item.get("item_sku", "")
                or item.get("ebay_item_number", "")
                or item.get("item_id", "")
                or item.get("item_asin", "")
                or ""
            ).strip()
            if key and key not in item_keys:
                item_keys.append(key)

            title = str(item.get("title", "") or "").strip()
            if title and title not in item_titles:
                item_titles.append(title)

        return {
            "label_pdf": str(label_pdf or ""),
            "status": "matched",
            "order_id": order.get("order_id", ""),
            "platform": order.get("platform", ""),
            "ship_name": order.get("ship_name", ""),
            "ship_postal": order.get("ship_postal", ""),
            "tracking_number": order.get("tracking_number", ""),
            "carrier": effective_carrier,
            "method": "manual_queue_resolution",
            "confidence": 1.0,
            "output_pdf": str(output_pdf),
            "process_index": process_index,
            "sort_label": sort_meta.get("label", ""),
            "sort_qty": sort_meta.get("qty", 0),
            "sort_item_key": sort_meta.get("item_key", ""),
            "sort_location": sort_meta.get("location", ""),
            "sort_carrier": sort_meta.get("carrier", ""),
            "item_count": len(items),
            "quantity_total": quantity_total,
            "total_paid": round(total_paid, 2),
            "item_keys": item_keys,
            "item_titles": item_titles,
        }

    def write_resolved_label_into_latest_batch(
        self,
        queue_entry: dict[str, Any],
        order: dict[str, Any],
        source_pdf: Path,
        idx: dict[tuple[str, str], dict[str, str]],
    ) -> dict[str, Any]:
        latest = self._latest_batch_dir()
        if latest is None:
            output_dir = self.settings.processed_root_folder / "manual_resolved"
            output_dir.mkdir(parents=True, exist_ok=True)
            out = self._render_one_label(source_pdf, order, idx, output_dir)
            return {"ok": True, "output_pdf": str(out), "batch_updated": False}

        output_dir = latest / "output_pdfs"
        output_dir.mkdir(parents=True, exist_ok=True)
        out = self._render_one_label(source_pdf, order, idx, output_dir)

        report_path = latest / "batch_report.json"
        report: dict[str, Any] = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = {}

        results = list(report.get("results", []) if isinstance(report, dict) else [])
        target_label_pdf = str(queue_entry.get("label_pdf", "") or source_pdf)
        existing_index = len(results)

        for i, row in enumerate(results):
            if str(row.get("label_pdf", "") or "") != target_label_pdf:
                continue
            try:
                existing_index = int(row.get("process_index", i) or i)
            except Exception:
                existing_index = i
            results[i] = self._build_matched_report_entry(
                label_pdf=target_label_pdf,
                source_pdf=source_pdf,
                order=order,
                output_pdf=out,
                idx=idx,
                process_index=existing_index,
            )
            break
        else:
            results.append(
                self._build_matched_report_entry(
                    label_pdf=target_label_pdf,
                    source_pdf=source_pdf,
                    order=order,
                    output_pdf=out,
                    idx=idx,
                    process_index=existing_index,
                )
            )

        report["results"] = results
        self._recount_batch_summary(report)
        atomic_write_json(report_path, report)
        self._save_resolution_override(latest, queue_entry, order)
        return {"ok": True, "output_pdf": str(out), "batch_updated": True, "batch_dir": str(latest)}

    def _build_label_identity(self, label_pdf: Path, signals: dict[str, Any]) -> str:
        parts: list[str] = []
        recipient = (signals.get("recipient_name") or "").strip()
        tracking = (signals.get("tracking_number") or "").strip()
        postal = (signals.get("ship_postal") or "").strip()
        order_id = (signals.get("order_id_amazon") or signals.get("order_id_ebay") or "").strip()

        if recipient:
            parts.append(recipient)
        if tracking:
            parts.append(f"TRK {tracking}")
        if order_id:
            parts.append(f"ORD {order_id}")
        if postal and (recipient or tracking):
            parts.append(f"ZIP {postal}")

        return " | ".join(parts) if parts else label_pdf.name

    def _variation_options(self, row: dict[str, str] | None) -> list[str]:
        raw = str((row or {}).get("variation_options", "") or "").strip()
        if not raw:
            return []
        out: list[str] = []
        for part in re.split(r"[|\n;,]+", raw):
            value = str(part or "").strip()
            if value and value not in out:
                out.append(value)
        return out

    def _order_variation_options(self, order: dict[str, Any], idx: dict[tuple[str, str], dict[str, str]]) -> list[str]:
        platform = str(order.get("platform", "")).lower().strip()
        for item in order.get("items", []) or []:
            row = self._find_row_for_item(platform, item, idx)
            options = self._variation_options(row)
            if len(options) >= 2:
                return options
        return []

    def _apply_variant_choice(self, order: dict[str, Any], idx: dict[tuple[str, str], dict[str, str]], variant_choice: str) -> dict[str, Any]:
        chosen = str(variant_choice or "").strip()
        if not chosen:
            return order
        clone = copy.deepcopy(order)
        platform = str(clone.get("platform", "")).lower().strip()
        for item in clone.get("items", []) or []:
            manual_row = item.get("_manual_row") if isinstance(item.get("_manual_row"), dict) else None
            row = manual_row or self._find_row_for_item(platform, item, idx)
            options = self._variation_options(row)
            if len(options) < 2 or chosen not in options:
                continue
            next_row = dict(row or {})
            next_row["custom_label"] = chosen
            item["_manual_row"] = next_row
            break
        return clone

    def _normalize_label_source(self, label_pdf: Path, order: dict[str, Any], output_dir: Path) -> Path:
        if str(order.get("platform", "")).lower().strip() != "ebay":
            return label_pdf
        layout = self.settings.config.get("print_layout", {})
        page_mode = str(layout.get("page_mode", "half_sheet_top"))
        try:
            with fitz.open(str(label_pdf)) as src:
                if src.page_count <= 0:
                    return label_pdf
                page = src[0]
                rect = page.rect
                clip = rect
                target = rect
                if page_mode == "half_sheet_top":
                    clip = fitz.Rect(0, 0, rect.width, rect.height / 2.0)
                    target = fitz.Rect(0, 0, rect.width, rect.height / 2.0)
                elif page_mode == "half_sheet_bottom":
                    clip = fitz.Rect(0, rect.height / 2.0, rect.width, rect.height)
                    target = fitz.Rect(0, rect.height / 2.0, rect.width, rect.height)
                tmp_dir = output_dir / "_normalized"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                out_path = tmp_dir / f"normalized_{sanitize_filename(label_pdf.stem)}.pdf"
                doc = fitz.open()
                new_page = doc.new_page(width=rect.width, height=rect.height)
                new_page.show_pdf_page(target, src, 0, clip=clip, rotate=180)
                doc.save(str(out_path))
                doc.close()
                return out_path
        except Exception:
            logging.exception("Failed to normalize eBay label orientation: %s", label_pdf)
        return label_pdf

    def _order_item_keys(self, item: dict[str, Any]) -> list[str]:
        iid = (item.get("item_id") or "").strip()
        sku = (item.get("item_sku") or "").strip()
        asin = (item.get("item_asin") or "").strip().upper()
        ebay_num = (item.get("ebay_item_number") or "").strip()
        # Prefer explicit platform IDs over legacy item_id to avoid cross-source collisions.
        return [k for k in [asin, sku, ebay_num, iid] if k and k.upper() != "UNKNOWN"]

    def _find_row_for_item(self, platform: str, item: dict[str, Any], idx: dict[tuple[str, str], dict[str, str]]) -> dict[str, str] | None:
        p = (platform or "").lower().strip()
        for k in self._order_item_keys(item):
            row = idx.get((p, k))
            if row is not None:
                return row
        return None

    def _effective_item_label(self, row: dict[str, str] | None, item: dict[str, Any]) -> str:
        if row is not None:
            custom = (row.get("custom_label") or "").strip()
            if custom:
                return custom
            db_title = (row.get("item_title") or "").strip()
            if db_title:
                return db_title
        return (item.get("title") or "").strip()


    def _carrier_from_tracking_number(self, tracking_number: str) -> str:
        tracking = re.sub(r"[^A-Za-z0-9]", "", str(tracking_number or "").upper())
        if not tracking:
            return ""
        if tracking.startswith("1Z"):
            return "ups"
        if tracking.startswith(("92", "93", "94", "95", "96")) and len(tracking) >= 20:
            return "usps"
        if tracking.isdigit() and len(tracking) in {12, 15, 20, 22}:
            return "fedex"
        return ""

    def _effective_carrier(self, order: dict[str, Any], extracted_carrier: str = "") -> str:
        carrier = str(extracted_carrier or "").strip().lower()
        if carrier:
            return carrier
        return self._carrier_from_tracking_number(str(order.get("tracking_number", "") or ""))

    def _sort_meta_for_order(self, order: dict[str, Any], idx: dict[tuple[str, str], dict[str, str]], carrier: str = "") -> dict[str, Any]:
        items = order.get("items", []) or []
        first = items[0] if items else {}
        platform = str(order.get("platform", "")).lower().strip()
        row = self._find_row_for_item(platform, first, idx) if first else None

        keys = self._order_item_keys(first) if first else []
        item_key = keys[0] if keys else ""

        qty_raw = first.get("quantity", 0) if isinstance(first, dict) else 0
        try:
            qty = int(float(qty_raw))
        except Exception:
            qty = 0

        label = self._effective_item_label(row, first) if isinstance(first, dict) else ""
        location = (row.get("location", "") if isinstance(row, dict) else "") or ""

        return {
            "label": str(label or "").strip(),
            "qty": qty,
            "item_key": str(item_key or "").strip(),
            "location": str(location or "").strip(),
            "carrier": str(carrier or "").strip().lower(),
        }

    def _output_sort_config(self) -> dict[str, Any]:
        cfg = self.settings.config.get("output_sort", {}) if isinstance(self.settings.config, dict) else {}
        mode = str(cfg.get("mode", "processed") or "processed").strip().lower()
        if mode not in {"processed", "processed_reverse", "custom"}:
            mode = "processed"

        pr = cfg.get("priority_fields", ["label", "location", "qty", "item_key"])
        priority_fields = [str(x).strip().lower() for x in (pr if isinstance(pr, list) else []) if str(x).strip()]
        if not priority_fields:
            priority_fields = ["label", "location", "qty", "item_key"]

        enabled = cfg.get("enabled_fields", {}) if isinstance(cfg.get("enabled_fields", {}), dict) else {}
        directions = cfg.get("directions", {}) if isinstance(cfg.get("directions", {}), dict) else {}

        return {
            "mode": mode,
            "priority_fields": priority_fields,
            "enabled_fields": {
                "label": bool(enabled.get("label", True)),
                "qty": bool(enabled.get("qty", False)),
                "item_key": bool(enabled.get("item_key", False)),
                "location": bool(enabled.get("location", False)),
                "carrier": bool(enabled.get("carrier", False)),
            },
            "directions": {
                "label": "desc" if str(directions.get("label", "asc")).lower() == "desc" else "asc",
                "qty": "desc" if str(directions.get("qty", "asc")).lower() == "desc" else "asc",
                "item_key": "desc" if str(directions.get("item_key", "asc")).lower() == "desc" else "asc",
                "location": "desc" if str(directions.get("location", "asc")).lower() == "desc" else "asc",
                "carrier": "desc" if str(directions.get("carrier", "asc")).lower() == "desc" else "asc",
            },
        }

    def _sort_value_for_entry(self, entry: dict[str, Any], field: str) -> Any:
        f = str(field or "").lower().strip()
        if f == "qty":
            try:
                return int(entry.get("sort_qty", 0) or 0)
            except Exception:
                return 0
        if f == "item_key":
            return str(entry.get("sort_item_key", "") or "").lower()
        if f == "location":
            return str(entry.get("sort_location", "") or "").lower()
        if f == "carrier":
            return str(entry.get("sort_carrier", "") or "").lower()
        return str(entry.get("sort_label", "") or "").lower()

    def _sorted_output_pdfs_from_report(self, batch_dir: Path, pdfs: list[Path]) -> list[Path]:
        report_path = batch_dir / "batch_report.json"
        report: dict[str, Any] = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = {}

        results = report.get("results", []) if isinstance(report, dict) else []
        matched = [r for r in results if str(r.get("status", "")).lower() == "matched"]

        by_name = {p.name: p for p in pdfs}
        entries: list[dict[str, Any]] = []
        for i, r in enumerate(matched):
            out_name = Path(str(r.get("output_pdf", "") or "")).name
            p = by_name.get(out_name)
            if p is None:
                continue
            row = dict(r)
            row["_pdf"] = p
            row["_process_index"] = int(r.get("process_index", i) or i)
            entries.append(row)

        if not entries:
            return pdfs

        # Start in processing order for deterministic custom sorting.
        entries.sort(key=lambda e: e.get("_process_index", 0))
        sort_cfg = self._output_sort_config()
        mode = sort_cfg.get("mode", "processed")

        if mode == "processed_reverse":
            entries.reverse()

        elif mode == "custom":
            enabled = sort_cfg.get("enabled_fields", {})
            directions = sort_cfg.get("directions", {})
            priorities = [f for f in sort_cfg.get("priority_fields", []) if str(f) in {"label", "qty", "item_key", "location", "carrier"}]
            active = [f for f in priorities if bool(enabled.get(f, False))]
            if active:
                for f in reversed(active):
                    rev = str(directions.get(f, "asc")) == "desc"
                    entries.sort(key=lambda e, ff=f: self._sort_value_for_entry(e, ff), reverse=rev)

        ordered = [e["_pdf"] for e in entries]
        included = {p.name for p in ordered}
        remaining = [p for p in sorted(pdfs, key=lambda x: x.name.lower()) if p.name not in included]
        return ordered + remaining
    def _resolve_item_rows(
        self,
        order: dict[str, Any],
        idx: dict[tuple[str, str], dict[str, str]],
        auto_add_missing_items: bool = True,
    ) -> list[dict[str, str]]:
        item_rows: list[dict[str, str]] = []
        platform = str(order.get("platform", "")).lower().strip()

        for item in order.get("items", []):
            manual_row = item.get("_manual_row")
            if isinstance(manual_row, dict):
                item_rows.append(manual_row)
                continue

            row = self._find_row_for_item(platform, item, idx)
            keys = self._order_item_keys(item)

            if row is None and keys and auto_add_missing_items:
                asin = (item.get("item_asin") or "").strip().upper()
                primary = keys[0]
                row = self.item_db.ensure_item(platform, primary, title=item.get("title", ""), item_asin=asin)
                for alias in [row.get("item_id", ""), row.get("ebay_item_number", ""), row.get("amazon_sku", ""), row.get("amazon_asin", "")]:
                    alias = (alias or "").strip()
                    if alias:
                        idx[(platform, alias)] = row

            if row is not None:
                item_rows.append(row)

        return item_rows

    def _validate_required_fields(
        self,
        order: dict[str, Any],
        idx: dict[tuple[str, str], dict[str, str]],
    ) -> tuple[bool, str]:
        items = order.get("items", []) or []
        if not items:
            return False, "missing_items"

        platform = str(order.get("platform", "")).lower().strip()
        for item in items:
            qty_raw = item.get("quantity", 0)
            try:
                qty = int(float(qty_raw))
            except Exception:
                qty = 0
            if qty <= 0:
                return False, "missing_or_invalid_qty"

            row = self._find_row_for_item(platform, item, idx)
            label = self._effective_item_label(row, item)
            if not label:
                return False, "missing_internal_label"

        return True, ""
    def _render_one_label(
        self,
        label_pdf: Path,
        order: dict[str, Any],
        idx: dict[tuple[str, str], dict[str, str]],
        output_dir: Path,
        auto_add_missing_items: bool = True,
    ) -> Path:
        working_pdf = self._normalize_label_source(label_pdf, order, output_dir)
        item_rows = self._resolve_item_rows(order, idx, auto_add_missing_items=auto_add_missing_items)

        lines = build_overlay_lines(order, item_rows, self.settings.config)
        page_w, page_h = get_page_size(working_pdf)
        primary_overlay, remaining = create_overlay_pdf(page_w, page_h, lines, self.settings.config, region="primary")

        layout = self.settings.config.get("print_layout", {})
        overlay_mode = str(layout.get("overlay_mode", "margin"))
        overflow_mode = str(layout.get("overflow_mode", "backside"))
        items = list(order.get("items") or [])
        summary_page_min_items = int(layout.get("summary_page_min_items", layout.get("compact_threshold", 4)))

        def _should_use_summary_page() -> bool:
            return summary_page_min_items <= 0 or len(items) > summary_page_min_items

        def _append_followup_page(base_pdf: Path, source_lines: list[str], overflow_lines: list[str]) -> None:
            followup_pdf = (
                create_summary_half_page(page_w, page_h, source_lines, self.settings.config)
                if _should_use_summary_page()
                else create_backside_pdf(page_w, page_h, overflow_lines, self.settings.config)
            )
            tmp_out = base_pdf.with_suffix(".tmp.pdf")
            append_backside_page(base_pdf, followup_pdf, tmp_out)
            tmp_out.replace(base_pdf)

        out_name = sanitize_filename(
            f"{order.get('platform','')}_{order.get('order_id','')}_{(order.get('items') or [{}])[0].get('item_id','')}_enhanced.pdf"
        )
        out_path = output_dir / out_name

        if overlay_mode == "both":
            overlays = [primary_overlay]
            panel_overlay, panel_remaining = create_info_panel_overlay_pdf(page_w, page_h, lines, self.settings.config)
            overlays.append(panel_overlay)
            if remaining and overflow_mode == "secondary_margin":
                secondary_overlay, remaining = create_overlay_pdf(page_w, page_h, remaining, self.settings.config, region="secondary")
                overlays.append(secondary_overlay)
            merge_overlays_on_first_page(working_pdf, overlays, out_path)
            extra_remaining = panel_remaining if panel_remaining else remaining
            if extra_remaining:
                _append_followup_page(out_path, lines, extra_remaining)
            return out_path

        if overlay_mode == "backside" and str(layout.get("page_mode", "full_page")).startswith("half_sheet_"):
            panel_overlay, remaining = create_info_panel_overlay_pdf(page_w, page_h, lines, self.settings.config)
            if remaining:
                merge_overlay_on_first_page(working_pdf, panel_overlay, out_path)
                _append_followup_page(out_path, lines, remaining)
            else:
                merge_overlay_on_first_page(working_pdf, panel_overlay, out_path)
            return out_path

        if remaining:
            if overflow_mode == "secondary_margin" and overlay_mode == "margin":
                secondary_overlay, still_remaining = create_overlay_pdf(page_w, page_h, remaining, self.settings.config, region="secondary")
                overlays = [primary_overlay, secondary_overlay]

                orientation = str(layout.get("orientation_mode", "normal"))
                primary_preset = str(layout.get("rotated_primary_preset", layout.get("placement_preset", "")))
                secondary_preset = str(layout.get("rotated_secondary_preset", ""))
                side_pair = {primary_preset, secondary_preset} == {"left_margin", "right_margin"}

                if still_remaining and orientation == "rotated_90" and side_pair:
                    spill_cfg = copy.deepcopy(self.settings.config)
                    spill_layout = spill_cfg.setdefault("print_layout", {})
                    spill_layout["orientation_mode"] = "normal"
                    spill_layout["placement_preset"] = "top_margin"
                    spill_layout["margin_box_height"] = int(
                        layout.get("spill_margin_box_height", layout.get("margin_box_height", 36))
                    )
                    spill_layout["edge_inset_x"] = float(layout.get("edge_inset_x", 8)) + float(layout.get("margin_box_width", 32)) + float(layout.get("spill_edge_inset_x", 8))
                    spill_layout["edge_inset_y"] = float(layout.get("spill_edge_inset_y", layout.get("edge_inset_y", 24)))
                    # Use dedicated spill font/spacing settings
                    spill_layout["font_size"] = int(layout.get("spill_font_size", layout.get("font_size", 11)))
                    spill_layout["line_spacing"] = int(layout.get("spill_line_spacing", layout.get("line_spacing", 14)))

                    top_spill_overlay, still_remaining = create_overlay_pdf(
                        page_w,
                        page_h,
                        still_remaining,
                        spill_cfg,
                        region="primary",
                        preset_override="top_margin",
                    )
                    overlays.append(top_spill_overlay)

                    if still_remaining:
                        bottom_spill_overlay, still_remaining = create_overlay_pdf(
                            page_w,
                            page_h,
                            still_remaining,
                            spill_cfg,
                            region="primary",
                            preset_override="bottom_margin",
                        )
                        overlays.append(bottom_spill_overlay)

                    # --- Auto-compact retry ---
                    # If lines still overflow after all 4 zones AND item count
                    # exceeds the compact threshold, rebuild in compact 2-per-line
                    # format and re-render from scratch.
                    items = order.get("items", []) or []
                    compact_threshold = int(layout.get("compact_threshold", 4))
                    if still_remaining and len(items) > compact_threshold:
                        compact_lines = build_compact_overlay_lines(order, item_rows, self.settings.config)
                        p_ov, c_remaining = create_overlay_pdf(page_w, page_h, compact_lines, self.settings.config, region="primary")
                        c_overlays = [p_ov]
                        s_ov, c_remaining = create_overlay_pdf(page_w, page_h, c_remaining, self.settings.config, region="secondary")
                        c_overlays.append(s_ov)
                        if c_remaining:
                            ts_ov, c_remaining = create_overlay_pdf(
                                page_w,
                                page_h,
                                c_remaining,
                                spill_cfg,
                                region="primary",
                                preset_override="top_margin",
                            )
                            c_overlays.append(ts_ov)
                        if c_remaining:
                            bs_ov, c_remaining = create_overlay_pdf(
                                page_w,
                                page_h,
                                c_remaining,
                                spill_cfg,
                                region="primary",
                                preset_override="bottom_margin",
                            )
                            c_overlays.append(bs_ov)
                        merge_overlays_on_first_page(working_pdf, c_overlays, out_path)
                        # Ultimate fallback: summary half-page
                        if c_remaining:
                            _append_followup_page(out_path, compact_lines, c_remaining)
                        return out_path

                    merge_overlays_on_first_page(working_pdf, overlays, out_path)
                    if still_remaining:
                        _append_followup_page(out_path, lines, still_remaining)
                else:
                    merge_overlays_on_first_page(working_pdf, overlays, out_path)
                    if still_remaining:
                        _append_followup_page(out_path, lines, still_remaining)
            else:
                if overlay_mode == "backside":
                    append_backside_page(working_pdf, create_backside_pdf(page_w, page_h, lines, self.settings.config), out_path)
                else:
                    merge_overlay_on_first_page(working_pdf, primary_overlay, out_path)
                    _append_followup_page(out_path, lines, remaining)
        else:
            if overlay_mode == "backside":
                append_backside_page(working_pdf, create_backside_pdf(page_w, page_h, lines, self.settings.config), out_path)
            else:
                merge_overlay_on_first_page(working_pdf, primary_overlay, out_path)

        return out_path
    def _archive_inputs(self, files: list[Path], archive_dir: Path) -> None:
        root = self.settings.incoming_batch_folder
        for src in files:
            if "_unzipped" in src.parts:
                continue
            if not src.exists():
                continue

            rel = src.relative_to(root)
            dst = archive_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(src), str(dst))
            except Exception:
                try:
                    shutil.copy2(str(src), str(dst))
                except Exception:
                    logging.exception("Failed to archive %s", src)
        unzipped = root / "_unzipped"
        if unzipped.exists():
            shutil.rmtree(unzipped, ignore_errors=True)

    def _load_unresolved_queue(self) -> list[dict[str, Any]]:
        if not self.unresolved_queue_path.exists():
            return []
        try:
            rows = json.loads(self.unresolved_queue_path.read_text(encoding="utf-8"))
        except Exception:
            return []

        if not isinstance(rows, list):
            return []

        for row in rows:
            if not isinstance(row, dict):
                continue
            current_identity = str(row.get("label_identity", "") or "").strip()
            needs_refresh = (not current_identity) or ("us postage" in current_identity.lower()) or current_identity.lower().startswith("zip ")
            if not needs_refresh:
                continue
            src = Path(str(row.get("label_pdf", "")))
            if src.exists():
                try:
                    sig = extract_label_signals(src)
                    row["label_identity"] = self._build_label_identity(src, sig)
                    row.setdefault("recipient_name", sig.get("recipient_name", ""))
                    row.setdefault("tracking_number", sig.get("tracking_number", ""))
                    row.setdefault("ship_postal", sig.get("ship_postal", ""))
                    continue
                except Exception:
                    pass
            row["label_identity"] = src.name if src.name else str(row.get("label_pdf", ""))

        return rows

    def _save_unresolved_queue(self, queue: list[dict[str, Any]]) -> None:
        atomic_write_json(self.unresolved_queue_path, queue)

    def _resolve_source_pdf_from_queue_entry(self, entry: dict[str, Any]) -> Path | None:
        src = Path(str(entry.get("label_pdf", "") or ""))
        if src.exists():
            return src

        name = src.name
        for p in self.settings.processed_root_folder.rglob(name):
            if "input_archive" in p.parts:
                return p

        m = re.match(r"^(?P<base>.+)__p(?P<page>\d+)$", src.stem)
        if not m:
            return None

        base = f"{m.group('base')}.pdf"
        page_idx = int(m.group("page")) - 1
        for p in self.settings.processed_root_folder.rglob(base):
            if "input_archive" not in p.parts:
                continue
            try:
                reader = PdfReader(str(p))
                if page_idx < 0 or page_idx >= len(reader.pages):
                    continue
                out_dir = self.settings.processed_root_folder / "manual_resolved" / "_recovered"
                out_dir.mkdir(parents=True, exist_ok=True)
                recovered = out_dir / f"{sanitize_filename(src.stem)}.pdf"
                writer = PdfWriter()
                writer.add_page(reader.pages[page_idx])
                with recovered.open("wb") as f:
                    writer.write(f)
                return recovered
            except Exception:
                logging.exception("Failed to recover split source PDF for unresolved entry: %s", entry.get("label_pdf", ""))
        return None

    def save_variation_choice(self, label_pdf: str, order_id: str, variant_choice: str) -> dict[str, Any]:
        queue = self._load_unresolved_queue()
        chosen = str(variant_choice or "").strip()
        if not chosen:
            return {"ok": False, "error": "Choose a variation first"}

        updated = False
        for row in queue:
            if str(row.get("label_pdf", "")) != str(label_pdf):
                continue
            if str(row.get("reason", "")) != "multi_variation_choice_required":
                return {"ok": False, "error": "This queue item is not a variation-choice item"}
            options = [str(x).strip() for x in (row.get("variation_options", []) or []) if str(x).strip()]
            if options and chosen not in options:
                return {"ok": False, "error": "Selected variation is not valid for this item"}
            row["selected_variation"] = chosen
            row["selected_order_id"] = str(order_id or row.get("selected_order_id", "") or "").strip()
            updated = True
            break

        if not updated:
            return {"ok": False, "error": "Queue entry not found"}
        self._save_unresolved_queue(queue)
        return {"ok": True}


    def save_variation_choices_bulk(self, choices: list[dict[str, str]]) -> dict[str, Any]:
        queue = self._load_unresolved_queue()
        saved = 0
        errors: list[str] = []
        by_label: dict[str, dict[str, str]] = {}
        for ch in choices:
            label_pdf = str(ch.get("label_pdf", "") or "").strip()
            if not label_pdf:
                continue
            by_label[label_pdf] = {
                "label_pdf": label_pdf,
                "order_id": str(ch.get("order_id", "") or "").strip(),
                "variant_choice": str(ch.get("variant_choice", "") or "").strip(),
            }
        if not by_label:
            return {"ok": False, "saved": 0, "errors": ["No variation choices provided"]}

        for row in queue:
            key = str(row.get("label_pdf", "") or "").strip()
            if not key or key not in by_label:
                continue
            if str(row.get("reason", "")) != "multi_variation_choice_required":
                continue
            chosen = by_label[key].get("variant_choice", "")
            if not chosen:
                continue
            options = [str(x).strip() for x in (row.get("variation_options", []) or []) if str(x).strip()]
            if options and chosen not in options:
                errors.append(f"Invalid variation for {Path(key).name}")
                continue
            row["selected_variation"] = chosen
            row["selected_order_id"] = by_label[key].get("order_id", "") or str(row.get("selected_order_id", "") or "")
            saved += 1

        self._save_unresolved_queue(queue)
        return {"ok": True, "saved": saved, "errors": errors}

    def resolve_selected_variations(self) -> dict[str, Any]:
        queue = self._load_unresolved_queue()
        if not queue:
            return {"ok": True, "generated": 0, "remaining": 0, "errors": [], "batch_updated": False}

        kept: list[dict[str, Any]] = []
        generated = 0
        errors: list[str] = []
        idx = self.item_db.index()
        batch_updated = False

        for row in queue:
            if str(row.get("reason", "")) != "multi_variation_choice_required":
                kept.append(row)
                continue

            chosen = str(row.get("selected_variation", "") or "").strip()
            if not chosen:
                kept.append(row)
                continue

            order_id = str(row.get("selected_order_id", "") or "").strip()
            order = None
            for c in row.get("candidates", []) or []:
                if str(c.get("order_id", "")) == order_id and c.get("order"):
                    order = c.get("order")
                    break
            if order is None:
                order = row.get("order")
            if order is None:
                errors.append("Missing order payload for variation row")
                kept.append(row)
                continue

            src = self._resolve_source_pdf_from_queue_entry(row)
            if src is None or not src.exists():
                errors.append(f"Source label PDF not found for {row.get('label_pdf', '')}")
                kept.append(row)
                continue

            try:
                order = self._apply_variant_choice(order, idx, chosen)
                write_result = self.write_resolved_label_into_latest_batch(row, order, src, idx)
                batch_updated = batch_updated or bool(write_result.get("batch_updated"))
                generated += 1
            except Exception:
                logging.exception("Failed to generate output for variation queue row: %s", row.get("label_pdf", ""))
                errors.append(f"Failed to generate output for {row.get('label_pdf', '')}")
                kept.append(row)

        self._save_unresolved_queue(kept)
        combined = self.combine_latest_output_pdfs() if generated > 0 and batch_updated else {}
        return {
            "ok": True,
            "generated": generated,
            "remaining": len(kept),
            "errors": errors,
            "batch_updated": batch_updated,
            "combined": combined,
        }

    def resolve_unmatched(self, label_pdf: str, order_id: str, variant_choice: str = "", merge_order_ids: list[str] | None = None) -> dict[str, Any]:
        queue = self._load_unresolved_queue()
        kept: list[dict[str, Any]] = []
        target: dict[str, Any] | None = None
        for entry in queue:
            if entry.get("label_pdf") == label_pdf and target is None:
                target = entry
            else:
                kept.append(entry)

        if target is None:
            return {"ok": False, "error": "Queue entry not found"}

        order = None
        for c in target.get("candidates", []):
            if c.get("order_id") == order_id:
                order = c.get("order")
                break
        if order is None:
            order = target.get("order")
        if order is None:
            return {"ok": False, "error": "Order ID not in candidates"}

        selected_merge_ids: list[str] = []
        for raw in merge_order_ids or []:
            oid = str(raw or "").strip()
            if oid and oid not in selected_merge_ids:
                selected_merge_ids.append(oid)
        if order_id and order_id not in selected_merge_ids:
            selected_merge_ids.insert(0, order_id)

        if len(selected_merge_ids) < 2 and str(order.get("platform", "") or "").strip().lower() == "ebay":
            selected_tracking = _norm_tracking_value(order.get("tracking_number", "") or "")
            if selected_tracking:
                auto_merge_ids: list[str] = []
                for candidate in target.get("candidates", []) or []:
                    candidate_order = candidate.get("order") if isinstance(candidate.get("order"), dict) else None
                    if candidate_order is None:
                        continue
                    candidate_tracking = _norm_tracking_value(candidate_order.get("tracking_number", "") or candidate.get("tracking_number", "") or "")
                    if candidate_tracking != selected_tracking:
                        continue
                    candidate_id = str(candidate.get("order_id", "") or candidate_order.get("order_id", "") or "").strip()
                    if candidate_id and candidate_id not in auto_merge_ids:
                        auto_merge_ids.append(candidate_id)
                if len(auto_merge_ids) >= 2 and order_id in auto_merge_ids:
                    selected_merge_ids = auto_merge_ids

        if len(selected_merge_ids) >= 2:
            merged = self._merged_order_from_queue_candidates(target, selected_merge_ids)
            if merged is None:
                return {"ok": False, "error": "Could not merge the selected eBay orders for this label"}
            order = merged

        if str(target.get("reason", "")) == "multi_variation_choice_required":
            return {"ok": False, "error": "Use variation queue actions to save choices first, then generate all selected variations."}

        src = self._resolve_source_pdf_from_queue_entry(target)
        if src is None or not src.exists():
            return {"ok": False, "error": "Source label PDF not found"}

        idx = self.item_db.index()
        write_result = self.write_resolved_label_into_latest_batch(target, order, src, idx)
        out = Path(str(write_result.get("output_pdf", "")))

        self._save_unresolved_queue(kept)
        combined = self.combine_latest_output_pdfs() if write_result.get("batch_updated") else {}
        return {
            "ok": True,
            "output_pdf": str(out),
            "remaining": len(kept),
            "batch_updated": bool(write_result.get("batch_updated")),
            "combined": combined,
            "merged_order_ids": selected_merge_ids,
        }

    def remove_unresolved_entry(self, label_pdf: str) -> bool:
        queue = self._load_unresolved_queue()
        kept = [q for q in queue if str(q.get("label_pdf", "")) != str(label_pdf)]
        if len(kept) == len(queue):
            return False
        self._save_unresolved_queue(kept)
        return True
    def clear_unresolved_queue(self) -> int:
        queue = self._load_unresolved_queue()
        count = len(queue)
        self._save_unresolved_queue([])
        return count

    def clear_staged_files(self) -> int:
        root = self.settings.incoming_batch_folder
        root.mkdir(parents=True, exist_ok=True)
        removed = 0
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            try:
                p.unlink()
                removed += 1
            except Exception:
                logging.exception("Failed to remove staged file: %s", p)

        unzipped = root / "_unzipped"
        if unzipped.exists():
            shutil.rmtree(unzipped, ignore_errors=True)

        return removed

    def _is_label_pdf(self, path: Path) -> bool:
        if path.suffix.lower() != ".pdf":
            return False
        return "packing slip" not in path.name.lower()

    def _latest_input_archive_dir(self) -> Path | None:
        latest = self._latest_batch_dir()
        if latest is None:
            return None
        archive = latest / "input_archive"
        if not archive.exists():
            return None
        return archive

    def latest_batch_reprocess_candidates(self) -> dict[str, Any]:
        latest = self._latest_batch_dir()
        archive = self._latest_input_archive_dir()
        if latest is None or archive is None:
            return {"ok": False, "error": "No processed batch archive found."}

        report_path = latest / "batch_report.json"
        report: dict[str, Any] = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = {}

        rows: list[dict[str, str]] = []
        for r in report.get("results", []) if isinstance(report, dict) else []:
            if str(r.get("status", "")).lower() != "matched":
                continue
            order_id = str(r.get("order_id", "") or "").strip()
            if not order_id:
                continue
            rows.append({
                "order_id": order_id,
                "platform": str(r.get("platform", "") or "").strip(),
                "label_pdf": str(r.get("label_pdf", "") or "").strip(),
                "ship_name": str(r.get("ship_name", "") or "").strip(),
                "ship_postal": str(r.get("ship_postal", "") or "").strip(),
                "tracking_number": str(r.get("tracking_number", "") or "").strip(),
            })

        archived_files = [p for p in archive.rglob("*") if p.is_file()]
        archived_file_names = [str(p.relative_to(archive)) for p in sorted(archived_files, key=_path_sort_key)]

        return {
            "ok": True,
            "batch_dir": str(latest),
            "archive_dir": str(archive),
            "labels": rows,
            "restage_file_count": len(archived_files),
            "restage_preview_names": archived_file_names[:12],
        }

    def _restage_from_archive(self, archive: Path, selected_label_names: set[str] | None = None) -> int:
        source_files = [p for p in archive.rglob("*") if p.is_file()]
        if not source_files:
            return 0

        self.clear_staged_files()
        root = self.settings.incoming_batch_folder
        copied = 0

        for src in source_files:
            rel = src.relative_to(archive)
            dst = root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)

            is_label = self._is_label_pdf(src)
            if selected_label_names is not None and is_label and src.name not in selected_label_names:
                continue

            if dst.exists():
                stem = dst.stem
                suffix = dst.suffix
                i = 2
                while True:
                    alt = dst.with_name(f"{stem}_{i}{suffix}")
                    if not alt.exists():
                        dst = alt
                        break
                    i += 1

            try:
                shutil.copy2(src, dst)
                copied += 1
            except Exception:
                logging.exception("Failed to restage archived input: %s", src)

        return copied

    def reprocess_latest_batch(self) -> dict[str, Any]:
        archive = self._latest_input_archive_dir()
        if archive is None:
            return {"ok": False, "error": "No processed batches found to reprocess."}

        copied = self._restage_from_archive(archive, selected_label_names=None)
        if copied <= 0:
            return {"ok": False, "error": "Could not restage files from latest archive."}

        source_batch_dir = archive.parent
        result = self.process_batch()
        reapplied = self._reapply_resolution_overrides(source_batch_dir)
        result["reapplied_resolutions"] = int(reapplied.get("applied", 0) or 0)
        result["remaining_resolution_overrides"] = int(reapplied.get("remaining", 0) or 0)
        result["restaged_files"] = copied
        result["source_batch_dir"] = str(source_batch_dir)
        return result

    def reprocess_selected_from_latest(self, selected_order_ids: list[str]) -> dict[str, Any]:
        archive = self._latest_input_archive_dir()
        latest = self._latest_batch_dir()
        if archive is None or latest is None:
            return {"ok": False, "error": "No processed batches found to reprocess."}
        if not selected_order_ids:
            return {"ok": False, "error": "Select at least one label PDF."}

        report_path = latest / "batch_report.json"
        report: dict[str, Any] = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = {}

        matched_rows = [
            r for r in (report.get("results", []) if isinstance(report, dict) else [])
            if str(r.get("status", "")).lower() == "matched"
        ]
        valid_ids = {str(r.get("order_id", "") or "").strip() for r in matched_rows}
        selected = {str(x or "").strip() for x in selected_order_ids if str(x or "").strip()}
        selected = {x for x in selected if x in valid_ids}
        if not selected:
            return {"ok": False, "error": "No valid selected order IDs found in latest batch."}

        selected_label_names = {
            Path(str(r.get("label_pdf", "") or "").strip()).name
            for r in matched_rows
            if str(r.get("order_id", "") or "").strip() in selected and str(r.get("label_pdf", "") or "").strip()
        }
        selected_label_names = {name for name in selected_label_names if name}
        if not selected_label_names:
            return {"ok": False, "error": "Could not find archived label PDFs for the selected rows."}

        copied = self._restage_from_archive(archive, selected_label_names=selected_label_names)
        if copied <= 0:
            return {"ok": False, "error": "Could not restage files from latest archive."}

        source_batch_dir = archive.parent
        result = self.process_batch()
        reapplied = self._reapply_resolution_overrides(source_batch_dir)
        result["reapplied_resolutions"] = int(reapplied.get("applied", 0) or 0)
        result["remaining_resolution_overrides"] = int(reapplied.get("remaining", 0) or 0)
        result["restaged_files"] = copied
        result["selected_labels"] = len(selected)
        result["selected_order_ids"] = sorted(selected)
        result["source_batch_dir"] = str(source_batch_dir)

        combined = self.combine_latest_output_pdfs(order_ids=selected)
        result["combined"] = combined
        return result

    def _latest_batch_dir(self) -> Path | None:
        batches = [p for p in self.settings.processed_root_folder.glob("batch_*") if p.is_dir()]
        if not batches:
            return None
        return max(batches, key=lambda p: p.stat().st_mtime)

    def latest_batch_snapshot(self) -> dict[str, Any]:
        batch_dir = self._latest_batch_dir()
        if not batch_dir:
            return {}

        report_path = batch_dir / "batch_report.json"
        report = {}
        if report_path.exists():
            try:
                report = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = {}

        results = report.get("results", []) if isinstance(report, dict) else []
        errors = [r for r in results if r.get("status") == "error"]
        unresolved = [r for r in results if r.get("status") == "unresolved"]
        matched = [r for r in results if r.get("status") == "matched"]

        combined_pdf = batch_dir / "output_pdfs" / "combined_print.pdf"
        return {
            "batch_dir": str(batch_dir),
            "summary": report.get("summary", {}),
            "errors": errors,
            "unresolved": unresolved,
            "matched": matched,
            "combined_pdf": str(combined_pdf) if combined_pdf.exists() else "",
        }

    def combine_latest_output_pdfs(self, order_ids: set[str] | None = None) -> dict[str, Any]:
        batch_dir = self._latest_batch_dir()
        if not batch_dir:
            return {"ok": False, "error": "No processed batches found."}

        output_dir = batch_dir / "output_pdfs"
        pdfs = [p for p in output_dir.glob("*.pdf") if p.name.lower() != "combined_print.pdf"]
        if order_ids:
            wanted = {str(x or "").strip() for x in order_ids if str(x or "").strip()}
            pdfs = [p for p in pdfs if any(f"_{oid}_" in p.name for oid in wanted)]
        if not pdfs:
            return {"ok": False, "error": "No output PDFs found in latest batch."}

        # Respect configured combine/print sort mode using batch report metadata.
        pdfs = self._sorted_output_pdfs_from_report(batch_dir, pdfs)

        writer = PdfWriter()
        for pdf in pdfs:
            try:
                reader = PdfReader(str(pdf))
                for page in reader.pages:
                    writer.add_page(page)
            except Exception:
                logging.exception("Failed to include PDF in combined print file: %s", pdf)

        combined_path = output_dir / "combined_print.pdf"
        with combined_path.open("wb") as f:
            writer.write(f)

        return {"ok": True, "path": str(combined_path), "count": len(pdfs)}

    def purge_archives(self, days: int) -> int:
        cutoff = datetime.now() - timedelta(days=days)
        removed = 0
        for p in self.settings.processed_root_folder.glob("batch_*"):
            if not p.is_dir():
                continue
            if datetime.fromtimestamp(p.stat().st_mtime) < cutoff:
                shutil.rmtree(p, ignore_errors=True)
                removed += 1
        return removed
