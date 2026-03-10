from __future__ import annotations

import csv
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pypdf import PdfReader


AMZ_ORDER_RE = re.compile(r"\b\d{3}-\d{7}-\d{7}\b")
ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
ASIN_RE = re.compile(r"\bB[0-9A-Z]{9}\b", re.IGNORECASE)
SKU_CODE_RE = re.compile(r"\b(?=[A-Z0-9-]*[A-Z])[A-Z0-9]{2,8}-[A-Z0-9]{2,8}-[A-Z0-9]{2,8}\b")
SKU_TOKEN_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]{1,63}$", re.IGNORECASE)
SKU_LINE_RE = re.compile(r"\bsku\b\s*[:#-]?\s*([A-Z0-9][A-Z0-9._-]{1,63})\b", re.IGNORECASE)
ASIN_LINE_RE = re.compile(r"\basin\b\s*[:#-]?\s*(B[0-9A-Z]{9})\b", re.IGNORECASE)
STREET_SUFFIX_RE = re.compile(r"\b(st|street|rd|road|ave|avenue|dr|drive|ln|lane|blvd|boulevard|ct|court|cir|circle|way|pkwy|parkway|trl|trail)\b", re.IGNORECASE)
CITY_STATE_ZIP_RE = re.compile(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b")
SCIENTIFIC_NOTATION_ID_RE = re.compile(r"^\d+(?:\.\d+)?E[+-]?\d+$", re.IGNORECASE)


def _normalize_ebay_item_number(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d{10,14}", raw):
        return raw
    if SCIENTIFIC_NOTATION_ID_RE.fullmatch(raw):
        try:
            dec = Decimal(raw)
            if dec == dec.to_integral_value():
                normalized = format(dec.quantize(Decimal("1")), "f")
                normalized = normalized.replace(".", "").strip()
                if re.fullmatch(r"\d{10,14}", normalized):
                    return normalized
        except (InvalidOperation, ValueError):
            return ""
        return ""
    return raw


def _normalize_ebay_variation_details(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    bracketed = re.findall(r"\[([^\]]+)\]", raw)
    if bracketed:
        parts: list[str] = []
        for part in bracketed:
            chunk = str(part or "").strip()
            if not chunk:
                continue
            if ":" in chunk:
                _, value = chunk.split(":", 1)
                chunk = value.strip()
            if chunk and chunk not in parts:
                parts.append(chunk)
        if parts:
            return " / ".join(parts)
    clean = raw.strip().strip("[]")
    if ":" in clean:
        _, value = clean.split(":", 1)
        clean = value.strip()
    return clean


def _money(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _looks_like_asin(value: str) -> bool:
    v = value.strip().upper()
    return bool(ASIN_RE.fullmatch(v))


def _extract_asin_from_text(text: str) -> str:
    m = ASIN_RE.search(text or "")
    return m.group(0).upper() if m else ""


def _extract_amazon_asin_from_row(row: dict[str, Any]) -> str:
    candidates = [
        _clean_text(row.get("asin")),
        _clean_text(row.get("product-id")),
        _clean_text(row.get("product_id")),
        _clean_text(row.get("merchant-product-id")),
        _clean_text(row.get("merchant_product_id")),
    ]
    for c in candidates:
        if _looks_like_asin(c):
            return c.upper()
    return ""


def _extract_amazon_sku_from_row(row: dict[str, Any]) -> str:
    return _clean_text(row.get("seller-sku")) or _clean_text(row.get("seller_sku")) or _clean_text(row.get("sku"))


def _best_amazon_item_id(row: dict[str, Any]) -> str:
    # Amazon TXT `sku` can be order-level in some reports; prefer ASIN as stable item key.
    asin = _extract_amazon_asin_from_row(row)
    if asin:
        return asin
    sku = _extract_amazon_sku_from_row(row)
    return sku or "UNKNOWN"
def _extract_sku_from_lines(lines: list[str]) -> tuple[str, int]:
    for i, raw in enumerate(lines):
        ln = raw.strip()
        if not ln:
            continue

        if "sku" in ln.lower():
            m = SKU_CODE_RE.search(ln.upper())
            if m:
                return m.group(0).upper(), i

        m = SKU_CODE_RE.fullmatch(ln.upper())
        if m:
            return m.group(0).upper(), i

    return "", -1
def _clean_candidate_token(value: str) -> str:
    v = str(value or "").strip().strip(":#- ").strip()
    return re.sub(r"[^\w.\-]", "", v)


def _extract_sku_asin_pairs(lines: list[str], full_text: str) -> list[dict[str, Any]]:
    sku_hits: list[tuple[int, str]] = []
    asin_hits: list[tuple[int, str]] = []

    for i, raw in enumerate(lines):
        ln = raw.strip()
        if not ln:
            continue

        m = SKU_LINE_RE.search(ln)
        if m:
            tok = _clean_candidate_token(m.group(1)).upper()
            if tok and SKU_TOKEN_RE.fullmatch(tok):
                sku_hits.append((i, tok))
        elif re.fullmatch(r"sku\s*[:#-]?", ln, re.IGNORECASE):
            for j in range(i + 1, min(i + 4, len(lines))):
                cand = _clean_candidate_token(lines[j]).upper()
                if cand and SKU_TOKEN_RE.fullmatch(cand) and not _looks_like_asin(cand):
                    sku_hits.append((j, cand))
                    break

        m = ASIN_LINE_RE.search(ln)
        if m:
            asin_hits.append((i, m.group(1).upper()))
        elif re.fullmatch(r"asin\s*[:#-]?", ln, re.IGNORECASE):
            for j in range(i + 1, min(i + 4, len(lines))):
                cand = _clean_candidate_token(lines[j]).upper()
                if _looks_like_asin(cand):
                    asin_hits.append((j, cand))
                    break

    if not asin_hits:
        asin = _extract_asin_from_text(full_text)
        if asin:
            asin_hits.append((-1, asin))

    # SKU-first pairing, then attach nearest ASIN. This avoids bad pairings when
    # slips contain extra ASIN/SKU-like tokens.
    pairs: list[dict[str, Any]] = []
    used_asin_idx: set[int] = set()

    def _choose_asin_for_sku(sku_line_idx: int) -> str:
        chosen_idx = -1
        chosen_dist = 10**9
        chosen_after_bias = 10**9
        for i, (asin_line_idx, _asin_val) in enumerate(asin_hits):
            if i in used_asin_idx:
                continue
            dist = abs(asin_line_idx - sku_line_idx) if asin_line_idx >= 0 and sku_line_idx >= 0 else 10**8
            # Prefer ASIN just after SKU when possible.
            after_bias = 0 if asin_line_idx >= sku_line_idx else 1
            if dist < chosen_dist or (dist == chosen_dist and after_bias < chosen_after_bias):
                chosen_idx = i
                chosen_dist = dist
                chosen_after_bias = after_bias
        if chosen_idx >= 0 and chosen_dist <= 12:
            used_asin_idx.add(chosen_idx)
            return asin_hits[chosen_idx][1]
        return ""

    for sku_line_idx, sku in sku_hits:
        asin = _choose_asin_for_sku(sku_line_idx)
        title = _extract_title_near_sku(lines, sku_line_idx)
        pairs.append(
            {
                "item_id": sku or asin or "UNKNOWN",
                "item_sku": sku,
                "item_asin": asin,
                "title": title or "Unknown Amazon Item",
                "quantity": 1,
                "line_total": 0.0,
            }
        )

    # If slip has ASIN but no SKU nearby, keep ASIN-only rows so DB can still learn.
    for i, (asin_line_idx, asin) in enumerate(asin_hits):
        if i in used_asin_idx:
            continue
        title = _extract_title_near_sku(lines, asin_line_idx)
        pairs.append(
            {
                "item_id": asin or "UNKNOWN",
                "item_sku": "",
                "item_asin": asin,
                "title": title or "Unknown Amazon Item",
                "quantity": 1,
                "line_total": 0.0,
            }
        )

    # Deduplicate exact (sku, asin) combos.
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in pairs:
        key = (str(row.get("item_sku", "")).strip(), str(row.get("item_asin", "")).strip())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _looks_like_title_line(ln: str) -> bool:
    s = ln.strip()
    if not s:
        return False
    lower = s.lower()

    skip_tokens = [
        "ship to",
        "sold by",
        "order",
        "address",
        "tracking",
        "sku",
        "qty",
        "quantity",
        "buyer",
        "recipient",
        "amazon",
        "thank",
        "postal",
        "apt",
        "suite",
    ]
    if any(tok in lower for tok in skip_tokens):
        return False
    if AMZ_ORDER_RE.search(s):
        return False
    if ZIP_RE.search(s):
        return False
    if CITY_STATE_ZIP_RE.search(s.upper()):
        return False

    if re.match(r"^\d+\s+", s) and STREET_SUFFIX_RE.search(s):
        return False

    if len(s) < 14:
        return False

    return True


def _extract_title_near_sku(lines: list[str], sku_idx: int) -> str:
    if not lines:
        return ""

    if sku_idx >= 0:
        # Prefer closest likely title line above SKU.
        for j in range(sku_idx - 1, max(-1, sku_idx - 7), -1):
            if j < 0:
                break
            cand = lines[j].strip()
            if _looks_like_title_line(cand):
                return cand

    for cand in lines:
        c = cand.strip()
        if _looks_like_title_line(c):
            return c
    return ""


def parse_amazon_tsv(path: Path, allowed_order_ids: set[str] | None = None) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            order_id = str(row.get("order-id", "")).strip()
            if not order_id:
                continue
            if allowed_order_ids is not None and order_id not in allowed_order_ids:
                continue

            rec = records.setdefault(
                order_id,
                {
                    "platform": "amazon",
                    "order_id": order_id,
                    "ship_name": str(row.get("recipient-name", "")).strip() or str(row.get("buyer-name", "")).strip(),
                    "ship_postal": str(row.get("ship-postal-code", "")).strip(),
                    "tracking_number": "",
                    "items": [],
                    "total_paid": 0.0,
                    "subtotal_paid": 0.0,
                    "item_subtotal_paid": 0.0,
                    "shipping_subtotal_paid": 0.0,
                    "service_prefix_text": "",
                },
            )

            item_sku = _extract_amazon_sku_from_row(row)
            item_asin = _extract_amazon_asin_from_row(row)
            item_id = _best_amazon_item_id(row)
            title = str(row.get("product-name", "")).strip()
            qty_raw = str(row.get("quantity-purchased", "1")).strip() or "1"
            try:
                qty = int(float(qty_raw))
            except Exception:
                qty = 1

            item_subtotal = _money(row.get("item-price")) - _money(row.get("item-promotion-discount"))
            shipping_subtotal = _money(row.get("shipping-price")) - _money(row.get("ship-promotion-discount"))
            line_subtotal = item_subtotal + shipping_subtotal
            line_total = (
                line_subtotal
                + _money(row.get("item-tax"))
                + _money(row.get("shipping-tax"))
            )

            service_level = str(row.get("ship-service-level", "") or "").strip()
            normalized_service = re.sub(r"\s+", "", service_level).lower()
            if service_level and normalized_service not in {"", "freeeconomy"} and not str(rec.get("service_prefix_text", "") or "").strip():
                rec["service_prefix_text"] = service_level

            rec["items"].append(
                {
                    "item_id": item_id,
                    "item_sku": item_sku,
                    "item_asin": item_asin,
                    "title": title,
                    "quantity": qty,
                    "line_total": line_total,
                    "line_subtotal": line_subtotal,
                    "item_subtotal": item_subtotal,
                    "shipping_subtotal": shipping_subtotal,
                }
            )
            rec["total_paid"] += line_total
            rec["subtotal_paid"] += line_subtotal
            rec["item_subtotal_paid"] += item_subtotal
            rec["shipping_subtotal_paid"] += shipping_subtotal
    return records
def parse_amazon_packing_slips(paths: list[Path], allowed_order_ids: set[str] | None = None) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for path in paths:
        try:
            reader = PdfReader(str(path))
        except Exception:
            continue
        for page in reader.pages:
            text = page.extract_text() or ""
            order_match = AMZ_ORDER_RE.search(text)
            if not order_match:
                continue
            order_id = order_match.group(0)
            if allowed_order_ids is not None and order_id not in allowed_order_ids:
                continue

            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            zip_match = ZIP_RE.search(text)
            items = _extract_sku_asin_pairs(lines, text)
            if not items:
                sku, sku_idx = _extract_sku_from_lines(lines)
                title = _extract_title_near_sku(lines, sku_idx)
                asin = _extract_asin_from_text(text)
                items = [
                    {
                        "item_id": sku or asin or "UNKNOWN",
                        "item_sku": sku,
                        "item_asin": asin or "",
                        "title": title or "Unknown Amazon Item",
                        "quantity": 1,
                        "line_total": 0.0,
                    }
                ]

            rec = records.setdefault(
                order_id,
                {
                    "platform": "amazon",
                    "order_id": order_id,
                    "ship_name": "",
                    "ship_postal": zip_match.group(1) if zip_match else "",
                    "tracking_number": "",
                    "items": [],
                    "total_paid": 0.0,
                },
            )
            rec["items"].extend(items)
    return records


def _detect_ebay_header_row(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for idx, line in enumerate(f):
            lower = line.lower()
            if "order number" in lower and "item number" in lower:
                return idx
    return 0


def parse_ebay_csv(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    header_row = _detect_ebay_header_row(path)
    records: dict[str, dict[str, Any]] = {}
    scientific_notation_rows = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for _ in range(header_row):
            next(f, None)
        reader = csv.DictReader(f)
        for row in reader:
            order_id = str(row.get("Order Number", "")).strip()
            if not order_id:
                continue

            rec = records.setdefault(
                order_id,
                {
                    "platform": "ebay",
                    "order_id": order_id,
                    "ship_name": str(row.get("Ship To Name", "")).strip() or str(row.get("Buyer Name", "")).strip(),
                    "ship_postal": str(row.get("Ship To Zip", "")).strip(),
                    "tracking_number": str(row.get("Tracking Number", "")).strip(),
                    "items": [],
                    "total_paid": 0.0,
                    "subtotal_paid": 0.0,
                    "item_subtotal_paid": 0.0,
                    "shipping_subtotal_paid": 0.0,
                    "_explicit_total": 0.0,
                    "_explicit_subtotal": 0.0,
                    "_explicit_item_subtotal": 0.0,
                    "_explicit_shipping_subtotal": 0.0,
                    "_item_total_sum": 0.0,
                    "_item_subtotal_sum": 0.0,
                    "_shipping_subtotal_sum": 0.0,
                    "_summary_qty": 0,
                },
            )

            raw_item_number = str(row.get("Item Number", "")).strip()
            item_id = _normalize_ebay_item_number(raw_item_number)
            if raw_item_number and not item_id and SCIENTIFIC_NOTATION_ID_RE.fullmatch(raw_item_number):
                scientific_notation_rows += 1
            title = str(row.get("Item Title", "")).strip()
            qty_raw = str(row.get("Quantity", "1")).strip() or "1"
            try:
                qty = int(float(qty_raw))
            except Exception:
                qty = 1

            explicit_total = _money(row.get("Total Price"))
            sold_for = _money(row.get("Sold For"))
            shipping = _money(row.get("Shipping And Handling"))
            explicit_subtotal = sold_for + shipping
            is_summary_only_row = not item_id and not title
            if is_summary_only_row:
                if explicit_total > 0:
                    rec["_explicit_total"] = max(float(rec.get("_explicit_total", 0.0) or 0.0), explicit_total)
                if explicit_subtotal > 0:
                    rec["_explicit_subtotal"] = max(float(rec.get("_explicit_subtotal", 0.0) or 0.0), explicit_subtotal)
                if sold_for > 0:
                    rec["_explicit_item_subtotal"] = max(float(rec.get("_explicit_item_subtotal", 0.0) or 0.0), sold_for)
                if shipping > 0:
                    rec["_explicit_shipping_subtotal"] = max(float(rec.get("_explicit_shipping_subtotal", 0.0) or 0.0), shipping)
                try:
                    rec["_summary_qty"] = max(int(rec.get("_summary_qty", 0) or 0), qty)
                except Exception:
                    pass
                continue

            line_total = explicit_total if explicit_total > 0 else (sold_for + shipping)
            line_subtotal = explicit_subtotal if explicit_subtotal > 0 else (sold_for + shipping)
            item_subtotal = sold_for
            shipping_subtotal = shipping

            variation_details_raw = str(row.get("Variation Details", "") or "").strip()
            variation_detail = _normalize_ebay_variation_details(variation_details_raw)

            rec["items"].append(
                {
                    "item_id": item_id,
                    "ebay_item_number": item_id,
                    "title": title,
                    "quantity": qty,
                    "line_total": line_total,
                    "line_subtotal": line_subtotal,
                    "item_subtotal": item_subtotal,
                    "shipping_subtotal": shipping_subtotal,
                    "variation_details": variation_details_raw,
                    "variation_detail": variation_detail,
                }
            )
            rec["_item_total_sum"] += line_total
            rec["_item_subtotal_sum"] += item_subtotal
            rec["_shipping_subtotal_sum"] += shipping_subtotal

    for rec in records.values():
        explicit_total = float(rec.pop("_explicit_total", 0.0) or 0.0)
        explicit_subtotal = float(rec.pop("_explicit_subtotal", 0.0) or 0.0)
        explicit_item_subtotal = float(rec.pop("_explicit_item_subtotal", 0.0) or 0.0)
        explicit_shipping_subtotal = float(rec.pop("_explicit_shipping_subtotal", 0.0) or 0.0)
        item_total_sum = float(rec.pop("_item_total_sum", 0.0) or 0.0)
        item_subtotal_sum = float(rec.pop("_item_subtotal_sum", 0.0) or 0.0)
        shipping_subtotal_sum = float(rec.pop("_shipping_subtotal_sum", 0.0) or 0.0)
        summary_qty = int(rec.pop("_summary_qty", 0) or 0)
        detail_qty = 0
        for item in rec.get("items", []) or []:
            try:
                detail_qty += int(item.get("quantity", 1) or 1)
            except Exception:
                detail_qty += 1

        missing_qty = max(0, summary_qty - detail_qty)
        if missing_qty > 0:
            for idx in range(missing_qty):
                rec.setdefault("items", []).append(
                    {
                        "item_id": f"EBAY-SUMMARY-MISSING-{idx + 1}",
                        "ebay_item_number": "",
                        "title": "eBay item details missing from OrdersReport",
                        "quantity": 1,
                        "line_total": 0.0,
                        "line_subtotal": 0.0,
                        "item_subtotal": 0.0,
                        "shipping_subtotal": 0.0,
                    }
                )

        rec["total_paid"] = explicit_total if explicit_total > 0 else item_total_sum
        rec["subtotal_paid"] = explicit_subtotal if explicit_subtotal > 0 else (item_subtotal_sum + shipping_subtotal_sum)
        rec["item_subtotal_paid"] = explicit_item_subtotal if explicit_item_subtotal > 0 else item_subtotal_sum
        rec["shipping_subtotal_paid"] = explicit_shipping_subtotal if explicit_shipping_subtotal > 0 else shipping_subtotal_sum

    warnings = {"scientific_notation_item_numbers": scientific_notation_rows} if scientific_notation_rows else {}
    return records, warnings





