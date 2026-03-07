from __future__ import annotations

import copy
import json
import logging
import re
import shutil
import zipfile
from functools import lru_cache
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter

from .item_db import ItemDB
from .label_text_extractor import extract_label_signals
from .label_matcher import match_label
from .order_parser import parse_amazon_packing_slips, parse_amazon_tsv, parse_ebay_csv
from .overlay_renderer import build_overlay_lines, create_backside_pdf, create_overlay_pdf, get_page_size
from .pdf_merge import append_backside_page, merge_overlay_on_first_page, merge_overlays_on_first_page
from .platform_detector import detect_platform_from_path, parse_order_id_from_filename
from .settings_manager import SettingsManager
from .utils import atomic_write_json, sanitize_filename


ORDER_ID_RE = re.compile(r"\d{3}-\d{7}-\d{7}")


class BatchManager:
    def __init__(self, settings: SettingsManager) -> None:
        self.settings = settings
        self.item_db = ItemDB(settings.items_csv_path, settings.config.get("new_item_defaults", {}))
        self.unresolved_queue_path = settings.processed_root_folder / "unresolved_queue.json"

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
        }

    def _all_batch_files(self) -> list[Path]:
        root = self.settings.incoming_batch_folder
        root.mkdir(parents=True, exist_ok=True)
        files = [p for p in root.rglob("*") if p.is_file()]
        return [p for p in files if "_split_pages" not in p.parts]

    def _extract_zip_files(self) -> list[Path]:
        extracted: list[Path] = []
        root = self.settings.incoming_batch_folder
        for z in root.glob("*.zip"):
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

    @lru_cache(maxsize=256)
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

    def _find_ebay_csv(self, files: list[Path]) -> Path | None:
        for p in files:
            if p.suffix.lower() == ".csv" and "ordersreport" in p.name.lower():
                return p
        return None

    def _looks_like_amazon_tsv(self, path: Path) -> bool:
        try:
            first = path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()[0].lower()
        except Exception:
            return False
        required = ["order-id", "sku", "quantity-purchased"]
        return all(r in first for r in required)

    def _find_amazon_txt(self, files: list[Path]) -> Path | None:
        txts = [p for p in files if p.suffix.lower() == ".txt"]
        # First pass: content-based detection (best).
        for p in txts:
            if self._looks_like_amazon_tsv(p):
                return p
        # Second pass: filename hint fallback.
        for p in txts:
            if "order report" in p.name.lower():
                return p
        return None

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
    def _build_orders(self, files: list[Path], label_pdfs: list[Path]) -> dict[str, dict[str, Any]]:
        ebay_csv = self._find_ebay_csv(files)
        amazon_txt = self._find_amazon_txt(files)
        packing_slips = self._find_packing_slips(files)

        orders: dict[str, dict[str, Any]] = {}
        if ebay_csv:
            orders.update(parse_ebay_csv(ebay_csv))

        amazon_label_pdfs = [p for p in label_pdfs if detect_platform_from_path(p) == "amazon"]
        amazon_ids = self._extract_amazon_order_ids_from_labels(amazon_label_pdfs)
        filter_by_ids = self._should_filter_amazon_report_by_label_ids(amazon_label_pdfs, amazon_ids)
        if amazon_txt:
            parsed = parse_amazon_tsv(amazon_txt, allowed_order_ids=amazon_ids if filter_by_ids else None)
            for rec in parsed.values():
                rec["source"] = "amazon_report"
            orders.update(parsed)

        if packing_slips:
            slip_rows = parse_amazon_packing_slips(packing_slips, allowed_order_ids=amazon_ids if filter_by_ids else None)
            if amazon_txt:
                self._enrich_amazon_orders_with_packing_slips(orders, slip_rows)
            else:
                for rec in slip_rows.values():
                    rec["source"] = "packing_slip"
                orders.update({k: v for k, v in slip_rows.items() if k not in orders})

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
        return orders

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
                else:
                    touched += 1

        return {"processed_slips": len(slips), "items_touched": touched}
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

        orders = self._build_orders(files, label_pdfs)
        if not orders:
            return {"ok": False, "error": "No order data found. Add eBay OrdersReport CSV and/or Amazon Order Report TXT."}

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_dir = self.settings.processed_root_folder / f"batch_{ts}"
        output_dir = batch_dir / "output_pdfs"
        archive_dir = batch_dir / "input_archive"
        output_dir.mkdir(parents=True, exist_ok=True)
        archive_dir.mkdir(parents=True, exist_ok=True)

        unresolved_queue = self._load_unresolved_queue()
        report: dict[str, Any] = {"timestamp": ts, "results": [], "summary": {"matched": 0, "unresolved": 0, "errors": 0}}
        idx = self.item_db.index()

        for label_pdf in label_pdfs:
            try:
                platform = detect_platform_from_path(label_pdf)
                direct_order = None
                if platform == "amazon":
                    file_order_id = parse_order_id_from_filename(label_pdf)
                    if file_order_id and file_order_id in orders:
                        direct_order = orders[file_order_id]

                if direct_order is not None:
                    m = {
                        "status": "matched",
                        "method": "filename_order_id_direct",
                        "confidence": 1.0,
                        "order": direct_order,
                        "candidates": [],
                    }
                else:
                    m = match_label(label_pdf, orders, platform_hint=platform if platform != "unknown" else "")

                if m["status"] != "matched":
                    label_signals = extract_label_signals(label_pdf)
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
                    label_signals = extract_label_signals(label_pdf)
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

                valid, validation_reason = self._validate_required_fields(order, idx)
                if not valid:
                    label_signals = extract_label_signals(label_pdf)
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

                label_signals = extract_label_signals(label_pdf)
                out_path = self._render_one_label(label_pdf, order, idx, output_dir)
                sort_meta = self._sort_meta_for_order(order, idx, label_signals.get("carrier", ""))
                report["results"].append(
                    {
                        "label_pdf": str(label_pdf),
                        "status": "matched",
                        "order_id": m["order"].get("order_id", ""),
                        "platform": m["order"].get("platform", ""),
                        "ship_name": m["order"].get("ship_name", ""),
                        "ship_postal": m["order"].get("ship_postal", ""),
                        "tracking_number": m["order"].get("tracking_number", ""),
                        "carrier": label_signals.get("carrier", ""),
                        "method": m.get("method", ""),
                        "confidence": m.get("confidence", 0),
                        "output_pdf": str(out_path),
                        "process_index": len(report["results"]),
                        "sort_label": sort_meta.get("label", ""),
                        "sort_qty": sort_meta.get("qty", 0),
                        "sort_item_key": sort_meta.get("item_key", ""),
                        "sort_location": sort_meta.get("location", ""),
                        "sort_carrier": sort_meta.get("carrier", ""),
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
        item_rows = self._resolve_item_rows(order, idx, auto_add_missing_items=auto_add_missing_items)

        lines = build_overlay_lines(order, item_rows, self.settings.config)
        page_w, page_h = get_page_size(label_pdf)
        primary_overlay, remaining = create_overlay_pdf(page_w, page_h, lines, self.settings.config, region="primary")

        layout = self.settings.config.get("print_layout", {})
        overlay_mode = str(layout.get("overlay_mode", "margin"))
        overflow_mode = str(layout.get("overflow_mode", "backside"))

        out_name = sanitize_filename(
            f"{order.get('platform','')}_{order.get('order_id','')}_{(order.get('items') or [{}])[0].get('item_id','')}_enhanced.pdf"
        )
        out_path = output_dir / out_name

        if remaining:
            if overflow_mode == "secondary_margin" and overlay_mode == "margin":
                secondary_overlay, still_remaining = create_overlay_pdf(page_w, page_h, remaining, self.settings.config, region="secondary")
                overlays = [primary_overlay, secondary_overlay]

                orientation = str(layout.get("orientation_mode", "normal"))
                primary_preset = str(layout.get("rotated_primary_preset", layout.get("placement_preset", "")))
                secondary_preset = str(layout.get("rotated_secondary_preset", ""))
                side_pair = {primary_preset, secondary_preset} == {"left_margin", "right_margin"}

                # For left/right margin mode: keep single-sided by spilling next into
                # upper-half top then upper-half bottom margins (no backside page).
                if still_remaining and orientation == "rotated_90" and side_pair:
                    spill_cfg = copy.deepcopy(self.settings.config)
                    spill_layout = spill_cfg.setdefault("print_layout", {})
                    spill_layout["orientation_mode"] = "normal"
                    spill_layout["placement_preset"] = "top_margin"
                    spill_layout["margin_box_height"] = int(
                        layout.get("secondary_strip_height", max(24, int(layout.get("margin_box_height", 36))))
                    )

                    top_spill_overlay, still_remaining = create_overlay_pdf(page_w, page_h, still_remaining, spill_cfg, region="primary")
                    overlays.append(top_spill_overlay)

                    if still_remaining:
                        spill_layout["placement_preset"] = "bottom_margin"
                        bottom_spill_overlay, still_remaining = create_overlay_pdf(page_w, page_h, still_remaining, spill_cfg, region="primary")
                        overlays.append(bottom_spill_overlay)

                    if still_remaining:
                        logging.warning("Overlay overflow truncated after side + top/bottom spill regions: %s", label_pdf)

                    merge_overlays_on_first_page(label_pdf, overlays, out_path)
                else:
                    merge_overlays_on_first_page(label_pdf, overlays, out_path)
                    if still_remaining:
                        backside_pdf = create_backside_pdf(page_w, page_h, still_remaining, self.settings.config)
                        tmp_out = out_path.with_suffix(".tmp.pdf")
                        append_backside_page(out_path, backside_pdf, tmp_out)
                        tmp_out.replace(out_path)
            else:
                if overlay_mode == "backside":
                    append_backside_page(label_pdf, create_backside_pdf(page_w, page_h, lines, self.settings.config), out_path)
                else:
                    merge_overlay_on_first_page(label_pdf, primary_overlay, out_path)
                    backside_pdf = create_backside_pdf(page_w, page_h, remaining, self.settings.config)
                    tmp_out = out_path.with_suffix(".tmp.pdf")
                    append_backside_page(out_path, backside_pdf, tmp_out)
                    tmp_out.replace(out_path)
        else:
            if overlay_mode == "backside":
                append_backside_page(label_pdf, create_backside_pdf(page_w, page_h, lines, self.settings.config), out_path)
            else:
                merge_overlay_on_first_page(label_pdf, primary_overlay, out_path)

        return out_path

    def _archive_inputs(self, files: list[Path], archive_dir: Path) -> None:
        root = self.settings.incoming_batch_folder
        for src in files:
            if "_unzipped" in src.parts:
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

    def resolve_unmatched(self, label_pdf: str, order_id: str) -> dict[str, Any]:
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
            return {"ok": False, "error": "Order ID not in candidates"}

        src = Path(label_pdf)
        if not src.exists():
            name = src.name
            for p in self.settings.processed_root_folder.rglob(name):
                if "input_archive" in p.parts:
                    src = p
                    break
        if not src.exists():
            return {"ok": False, "error": "Source label PDF not found"}

        output_dir = self.settings.processed_root_folder / "manual_resolved"
        output_dir.mkdir(parents=True, exist_ok=True)
        idx = self.item_db.index()
        out = self._render_one_label(src, order, idx, output_dir)

        self._save_unresolved_queue(kept)
        return {"ok": True, "output_pdf": str(out)}
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

        return {
            "ok": True,
            "batch_dir": str(latest),
            "archive_dir": str(archive),
            "labels": rows,
        }

    def _restage_from_archive(self, archive: Path, selected_labels: set[Path] | None = None) -> int:
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
            if selected_labels is not None and is_label and src not in selected_labels:
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

        copied = self._restage_from_archive(archive, selected_labels=None)
        if copied <= 0:
            return {"ok": False, "error": "Could not restage files from latest archive."}

        result = self.process_batch()
        result["restaged_files"] = copied
        result["source_batch_dir"] = str(archive.parent)
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

        valid_ids = {str(r.get("order_id", "") or "").strip() for r in (report.get("results", []) if isinstance(report, dict) else []) if str(r.get("status", "")).lower() == "matched"}
        selected = {str(x or "").strip() for x in selected_order_ids if str(x or "").strip()}
        selected = {x for x in selected if x in valid_ids}
        if not selected:
            return {"ok": False, "error": "No valid selected order IDs found in latest batch."}

        copied = self._restage_from_archive(archive, selected_labels=None)
        if copied <= 0:
            return {"ok": False, "error": "Could not restage files from latest archive."}

        result = self.process_batch()
        result["restaged_files"] = copied
        result["selected_labels"] = len(selected)
        result["selected_order_ids"] = sorted(selected)
        result["source_batch_dir"] = str(archive.parent)

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
