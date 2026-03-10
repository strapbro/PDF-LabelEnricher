from __future__ import annotations

import io
import os
import re
from pathlib import Path
from typing import Any

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from pypdf import PdfReader


INLINE_LOCK_PREFIX = "@@INLINE@@ "

_COMIC_FONT_NAME = "ComicSansMS"
_COMIC_FONT_TRIED = False
_COMIC_FONT_OK = False

def _ensure_comic_font_registered() -> bool:
    global _COMIC_FONT_TRIED, _COMIC_FONT_OK
    if _COMIC_FONT_TRIED:
        return _COMIC_FONT_OK
    _COMIC_FONT_TRIED = True
    candidates = [
        Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "comic.ttf",
        Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "Comic Sans MS.ttf",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            pdfmetrics.registerFont(TTFont(_COMIC_FONT_NAME, str(path)))
            _COMIC_FONT_OK = True
            return True
        except Exception:
            continue
    _COMIC_FONT_OK = False
    return False

def _resolve_font_name(layout: dict[str, Any]) -> str:
    requested = str(layout.get("font_name", "Helvetica-Bold"))
    if not bool(layout.get("comic_mode", False)):
        return requested
    if _ensure_comic_font_registered():
        return _COMIC_FONT_NAME
    return requested


def _strip_emph_markers(text: str) -> tuple[str, bool]:
    if text.startswith(INLINE_LOCK_PREFIX):
        text = text[len(INLINE_LOCK_PREFIX) :]
    return text, False


def _text_width(text: str, font_name: str, font_size: int) -> float:
    clean, _ = _strip_emph_markers(text)
    return pdfmetrics.stringWidth(clean, font_name, font_size)


def _display_item_variation(item: dict[str, Any]) -> str:
    raw = str(item.get("variation_detail") or item.get("variation_details") or "").strip()
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


def _append_item_variation(base: str, item: dict[str, Any]) -> str:
    text = str(base or "").strip()
    variation = _display_item_variation(item)
    if not variation:
        return text
    if not text:
        return variation
    if variation.lower() in text.lower():
        return text
    return f"{text} [{variation}]"


def _format_manual_prefix(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    clean = re.sub(r"\s+", " ", clean).upper()
    return f"! {clean} !"



def _selected_order_total(order: dict[str, Any], config: dict[str, Any]) -> float | None:
    layout = config.get("print_layout", {}) if isinstance(config, dict) else {}
    mode = str(layout.get("total_display_mode", "grand_total") or "grand_total").strip().lower()

    if mode == "subtotal":
        preferred = order.get("subtotal_paid")
        fallback = order.get("total_paid")
    else:
        preferred = order.get("total_paid")
        fallback = order.get("subtotal_paid")

    for candidate in (preferred, fallback):
        try:
            value = float(candidate or 0)
        except Exception:
            continue
        if value > 0:
            return value
    return None


def _selected_total_label(config: dict[str, Any]) -> str:
    layout = config.get("print_layout", {}) if isinstance(config, dict) else {}
    mode = str(layout.get("total_display_mode", "grand_total") or "grand_total").strip().lower()
    return "SUBTOTAL" if mode == "subtotal" else "TOTAL"


def _money_line(label: str, value: Any, show_labels: bool) -> str:
    try:
        amount = float(value or 0)
    except Exception:
        return ""
    if amount <= 0:
        return ""
    return f"{label} ${amount:.2f}" if show_labels else f"${amount:.2f}"


def _auto_overlay_prefix_text(order: dict[str, Any]) -> str:
    prefixes: list[str] = []
    manual_prefix = str(order.get("manual_prefix_text", "") or "").strip()
    if manual_prefix:
        prefixes.append(manual_prefix)

    service_prefix = str(order.get("service_prefix_text", "") or "").strip()
    if service_prefix:
        prefixes.append(service_prefix)

    replacement = any(float(item.get("item_subtotal", 0) or 0) <= 0 for item in (order.get("items", []) or []))
    if replacement:
        prefixes.append("REPLACEMENT")

    deduped: list[str] = []
    seen: set[str] = set()
    for prefix in prefixes:
        key = re.sub(r"\s+", " ", str(prefix or "").strip()).upper()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return " ".join(_format_manual_prefix(prefix) for prefix in deduped)


def _with_continuation_notice(text: str, font_name: str, font_size: int, max_width: float, suffix: str = "!! -- CONT BELOW -- !!") -> str:
    clean, _ = _strip_emph_markers(text)
    suffix = str(suffix or "!! -- CONT BELOW -- !!").strip().upper()
    if not clean:
        return suffix
    candidate = f"{clean} {suffix}"
    if pdfmetrics.stringWidth(candidate, font_name, font_size) <= max_width:
        return candidate
    ellipsis = "..."
    reserved = pdfmetrics.stringWidth(f"{ellipsis} {suffix}", font_name, font_size)
    available = max_width - reserved
    if available <= 0:
        return _fit_text(suffix, font_name, font_size, max_width)
    trimmed = clean
    while trimmed and pdfmetrics.stringWidth(trimmed, font_name, font_size) > available:
        trimmed = trimmed[:-1].rstrip()
    if not trimmed:
        return _fit_text(suffix, font_name, font_size, max_width)
    return f"{trimmed}{ellipsis} {suffix}"
def _fit_text(text: str, font_name: str, font_size: int, max_width: float) -> str:
    clean, emph = _strip_emph_markers(text)
    if _text_width(text, font_name, font_size) <= max_width:
        return text
    suffix = "..."
    for i in range(len(clean), 0, -1):
        test_clean = clean[:i].rstrip() + suffix
        wrapped = f"*{test_clean}*" if emph else test_clean
        if _text_width(wrapped, font_name, font_size) <= max_width:
            return wrapped
    return f"*{suffix}*" if emph else suffix


def _wrap_text_word(text: str, font_name: str, font_size: int, max_width: float) -> list[str]:
    clean, emph = _strip_emph_markers(text)
    words = clean.split()
    if not words:
        return [text]
    out: list[str] = []
    cur = words[0]
    for w in words[1:]:
        cand = f"{cur} {w}"
        cand_wrapped = f"*{cand}*" if emph else cand
        if _text_width(cand_wrapped, font_name, font_size) <= max_width:
            cur = cand
        else:
            out.append(f"*{cur}*" if emph else cur)
            cur = w
    out.append(f"*{cur}*" if emph else cur)

    final: list[str] = []
    for line in out:
        if _text_width(line, font_name, font_size) <= max_width:
            final.append(line)
        else:
            final.extend(_wrap_text_char(line, font_name, font_size, max_width))
    return final


def _wrap_text_char(text: str, font_name: str, font_size: int, max_width: float) -> list[str]:
    clean, emph = _strip_emph_markers(text)
    if not clean:
        return [text]
    out: list[str] = []
    cur = ""
    for ch in clean:
        cand = cur + ch
        cand_wrapped = f"*{cand}*" if emph else cand
        if cur and _text_width(cand_wrapped, font_name, font_size) > max_width:
            out.append(f"*{cur}*" if emph else cur)
            cur = ch
        else:
            cur = cand
    if cur:
        out.append(f"*{cur}*" if emph else cur)
    return out


def _expand_lines(lines: list[str], font_name: str, font_size: int, max_width: float, wrap_mode: str) -> list[str]:
    expanded: list[str] = []
    for line in lines:
        if line.startswith(INLINE_LOCK_PREFIX):
            expanded.append(_fit_text(line[len(INLINE_LOCK_PREFIX) :], font_name, font_size, max_width))
            continue
        if wrap_mode == "word":
            expanded.extend(_wrap_text_word(line, font_name, font_size, max_width))
        elif wrap_mode == "char":
            expanded.extend(_wrap_text_char(line, font_name, font_size, max_width))
        else:
            expanded.append(_fit_text(line, font_name, font_size, max_width))
    return expanded


def _auto_perpendicular_primary(base_preset: str) -> str:
    if base_preset in ("top_margin", "bottom_margin"):
        return "left_margin"
    if base_preset in ("left_margin", "right_margin"):
        return "top_margin"
    return "left_margin"


def _auto_perpendicular_secondary(base_preset: str) -> str:
    if base_preset in ("top_margin", "bottom_margin"):
        return "right_margin"
    if base_preset in ("left_margin", "right_margin"):
        return "bottom_margin"
    return "right_margin"


def _resolve_preset(layout: dict[str, Any], region: str) -> str:
    orientation = str(layout.get("orientation_mode", "normal"))
    base = str(layout.get("placement_preset", "right_margin"))
    if orientation != "rotated_90":
        return base

    if region == "secondary":
        rp = str(layout.get("rotated_secondary_preset", "auto_perpendicular"))
        if rp == "auto_perpendicular":
            return _auto_perpendicular_secondary(base)
        return rp

    rp = str(layout.get("rotated_primary_preset", "auto_perpendicular"))
    if rp == "auto_perpendicular":
        return _auto_perpendicular_primary(base)
    return rp


def _safe_rect(config: dict[str, Any], page_w: float, page_h: float, preset_override: str | None = None) -> tuple[float, float, float, float]:
    layout = config["print_layout"]
    preset = preset_override or str(layout.get("placement_preset", "right_margin"))
    custom = layout.get("overlay_safe_rect", {})

    edge_x = float(layout.get("edge_inset_x", 8))
    edge_y = float(layout.get("edge_inset_y", 24))
    box_w = float(layout.get("margin_box_width", 220))
    box_h = float(layout.get("margin_box_height", max(120, page_h - (2 * edge_y))))
    page_mode = str(layout.get("page_mode", "half_sheet_top"))

    if preset == "custom":
        x = float(custom.get("x", 40))
        y = float(custom.get("y", 40))
        w = float(custom.get("w", 260))
        h = float(custom.get("h", 180))
        return (x, y, w, h)

    if page_mode == "half_sheet_top":
        region_y0 = page_h / 2.0
        region_y1 = page_h
    elif page_mode == "half_sheet_bottom":
        region_y0 = 0.0
        region_y1 = page_h / 2.0
    else:
        region_y0 = 0.0
        region_y1 = page_h

    region_h = max(8.0, region_y1 - region_y0)
    usable_h = max(4.0, min(box_h, region_h - (2 * edge_y)))
    usable_w = max(4.0, min(box_w, page_w - (2 * edge_x)))

    if preset == "left_margin":
        return (edge_x, region_y0 + edge_y, usable_w, usable_h)
    if preset == "top_margin":
        return (edge_x, region_y1 - edge_y - usable_h, page_w - (2 * edge_x), usable_h)
    if preset == "bottom_margin":
        return (edge_x, region_y0 + edge_y, page_w - (2 * edge_x), usable_h)

    return (page_w - edge_x - usable_w, region_y0 + edge_y, usable_w, usable_h)


def _secondary_rect(config: dict[str, Any], page_w: float, page_h: float) -> tuple[float, float, float, float]:
    layout = config["print_layout"]
    edge_x = float(layout.get("edge_inset_x", 8))
    edge_y = float(layout.get("edge_inset_y", 24))
    strip_h = float(layout.get("secondary_strip_height", 36))
    gap = float(layout.get("secondary_strip_gap", 6))
    anchor = str(layout.get("secondary_anchor", "midline"))
    page_mode = str(layout.get("page_mode", "half_sheet_top"))

    if page_mode == "half_sheet_top":
        region_y0 = page_h / 2.0
        region_y1 = page_h
    elif page_mode == "half_sheet_bottom":
        region_y0 = 0.0
        region_y1 = page_h / 2.0
    else:
        region_y0 = 0.0
        region_y1 = page_h

    region_h = max(8.0, region_y1 - region_y0)
    w = max(40.0, page_w - (2 * edge_x))
    h = max(4.0, min(strip_h, region_h - (2 * edge_y)))
    x = edge_x

    if anchor == "bottom_margin":
        y = region_y0 + edge_y
        return (x, y, w, h)

    # midline means "near the divider line" within the active page half.
    if page_mode == "half_sheet_top":
        y = min(region_y1 - h - edge_y, region_y0 + gap)
    elif page_mode == "half_sheet_bottom":
        y = max(region_y0 + edge_y, region_y1 - h - gap)
    else:
        mid = page_h / 2.0
        y = min(page_h - h - edge_y, max(edge_y, mid + gap))

    return (x, y, w, h)


def _draw_text_line(c: canvas.Canvas, x: float, y: float, w: float, text: str, align: str, font_name: str, font_size: int) -> None:
    clean, emph = _strip_emph_markers(text)
    tw = pdfmetrics.stringWidth(clean, font_name, font_size)

    if align == "center":
        draw_x = x + (w - tw) / 2.0
    elif align == "right":
        draw_x = x + w - tw
    else:
        draw_x = x

    c.drawString(draw_x, y, clean)
    if emph:
        c.setLineWidth(1)
        c.line(draw_x, y - 1.5, draw_x + tw, y - 1.5)


def _draw_text_line_local(c: canvas.Canvas, y: float, w: float, text: str, align: str, font_name: str, font_size: int) -> None:
    clean, emph = _strip_emph_markers(text)
    tw = pdfmetrics.stringWidth(clean, font_name, font_size)

    if align == "center":
        draw_x = (w - tw) / 2.0
    elif align == "right":
        draw_x = w - tw
    else:
        draw_x = 0

    c.drawString(draw_x, y, clean)
    if emph:
        c.setLineWidth(1)
        c.line(draw_x, y - 1.5, draw_x + tw, y - 1.5)


def _item_row_matches(row: dict[str, str], item: dict[str, Any]) -> bool:
    keys = [
        (item.get("item_id") or "").strip(),
        (item.get("item_sku") or "").strip(),
        (item.get("item_asin") or "").strip(),
        (item.get("ebay_item_number") or "").strip(),
    ]
    row_keys = [
        (row.get("item_id") or "").strip(),
        (row.get("ebay_item_number") or "").strip(),
        (row.get("amazon_sku") or "").strip(),
        (row.get("amazon_asin") or "").strip(),
    ]
    keyset = {k for k in keys if k}
    rowset = {k for k in row_keys if k}
    return bool(keyset & rowset)


def build_overlay_lines(order: dict[str, Any], item_rows: list[dict[str, str]], config: dict[str, Any]) -> list[str]:
    layout = config.get("print_layout", {})
    show_labels = layout.get("show_field_labels", True)
    lines: list[str] = []

    default_order = ["label", "qty", "total", "location", "title"]
    raw_order = layout.get("field_order", default_order)
    if isinstance(raw_order, str):
        tokens = [t.strip().lower() for t in raw_order.split(",") if t.strip()]
    elif isinstance(raw_order, list):
        tokens = [str(t).strip().lower() for t in raw_order if str(t).strip()]
    else:
        tokens = default_order

    allowed = ["label", "qty", "total", "subtotal", "item_subtotal", "shipping_subtotal", "location", "title"]
    field_order = [t for t in tokens if t in allowed]
    for t in allowed:
        if t not in field_order:
            field_order.append(t)

    inline_raw = str(layout.get("inline_fields_csv", "")).strip().lower()
    inline_fields = {x.strip() for x in inline_raw.split(",") if x.strip()} if inline_raw else set()
    inline_fields = {x for x in inline_fields if x in allowed}
    inline_sep = str(layout.get("inline_separator", " | "))

    line_groups_raw = str(layout.get("line_groups_csv", "")).strip().lower()
    line_groups: list[list[str]] = []
    if line_groups_raw:
        for grp in line_groups_raw.split(";"):
            fields = [x.strip().lower() for x in grp.split(",") if x.strip()]
            fields = [x for x in fields if x in allowed]
            if fields:
                line_groups.append(fields)
    defer_summary_line = line_groups_raw.replace(" ", "") == "qty,label;total,location"

    items = order.get("items", []) or []
    use_numbering = len(items) > 1
    item_locations: list[str] = []
    for item in items:
        row = next((r for r in item_rows if _item_row_matches(r, item)), None)
        item_locations.append(str((row or {}).get("location", "") or "").strip())

    inline_location_per_item = (
        any("location" in grp and any(f in grp for f in ("label", "qty", "title")) for grp in line_groups)
        or ("location" in inline_fields and any(f in inline_fields for f in ("label", "qty", "title")))
    )

    def _format_location_text(loc: str, index: int | None = None) -> str:
        clean = str(loc or "").strip()
        if not clean:
            return ""
        prefix = f"{index}) " if index is not None else ""
        return (f"{prefix}LOC {clean}") if show_labels else f"{prefix}{clean}"

    summary_locations: list[str] = []
    for idx, loc in enumerate(item_locations, start=1):
        if not loc:
            continue
        loc_text = _format_location_text(loc, idx if use_numbering else None)
        if loc_text and loc_text not in summary_locations:
            summary_locations.append(loc_text)
    summary_location_line = " | ".join(summary_locations) if summary_locations else ""

    total = _selected_order_total(order, config)
    total_label = _selected_total_label(config)
    total_line = _money_line(total_label, total, show_labels)
    subtotal_line = _money_line("SUBTOTAL", order.get("subtotal_paid"), show_labels)
    item_subtotal_line = _money_line("ITEM SUBTOTAL", order.get("item_subtotal_paid"), show_labels)
    shipping_subtotal_line = _money_line("SHIPPING SUBTOTAL", order.get("shipping_subtotal_paid"), show_labels)

    location_emitted = False
    total_emitted = False
    subtotal_emitted = False
    item_subtotal_emitted = False
    shipping_subtotal_emitted = False

    def _flag_on(value: Any, default: bool = True) -> bool:
        if value is None:
            return default
        s = str(value).strip().lower()
        if not s:
            return default
        return s not in ("0", "false", "off", "no")

    def _emit_with_inline(chunks: list[tuple[str, str]]) -> None:
        inline_parts: list[str] = []
        for field, text in chunks:
            text = (text or "").strip()
            if not text:
                continue
            if field in inline_fields:
                inline_parts.append(text)
                continue
            if inline_parts:
                lines.append(INLINE_LOCK_PREFIX + inline_sep.join(inline_parts))
                inline_parts = []
            lines.append(text)
        if inline_parts:
            lines.append(INLINE_LOCK_PREFIX + inline_sep.join(inline_parts))

    def _emit_with_line_groups(chunks: list[tuple[str, str]]) -> None:
        used_idx: set[int] = set()
        for grp in line_groups:
            grp_parts: list[str] = []
            for gf in grp:
                for idx, (field, text) in enumerate(chunks):
                    if idx in used_idx:
                        continue
                    if field != gf:
                        continue
                    text = (text or "").strip()
                    if text:
                        grp_parts.append(text)
                        used_idx.add(idx)
                    break
            if grp_parts:
                lines.append(INLINE_LOCK_PREFIX + inline_sep.join(grp_parts))

    for idx, item in enumerate(items, start=1):
        title = (item.get("title") or "").strip()
        qty = int(item.get("quantity", 1) or 1)
        row = next((r for r in item_rows if _item_row_matches(r, item)), None)
        item_location = item_locations[idx - 1] if idx - 1 < len(item_locations) else ""

        label = _append_item_variation(
            (
                (row or {}).get("custom_label", "").strip()
                or (row or {}).get("item_title", "").strip()
                or (item.get("item_sku") or "").strip()
                or (item.get("item_id") or "").strip()
                or title
            ),
            item,
        )
        prefix = f"{idx}) " if use_numbering else ""

        qty_line = f"QTY {qty}"
        if qty > 1:
            qty_line = f"*QTY {qty}*"

        chunks: list[tuple[str, str]] = []

        for field in field_order:
            if field == "location":
                if defer_summary_line:
                    continue
                if inline_location_per_item:
                    loc_text = _format_location_text(item_location)
                    if loc_text:
                        chunks.append(("location", loc_text))
                    continue
                if location_emitted:
                    continue
                if summary_location_line:
                    chunks.append(("location", summary_location_line))
                location_emitted = True
            elif field == "label":
                if label:
                    chunks.append(("label", f"{prefix}{label}"))
            elif field == "qty":
                chunks.append(("qty", qty_line))
            elif field == "title":
                if row is not None and not _flag_on(row.get("show_title", "0"), default=False):
                    continue
                title_text = _append_item_variation(title, item)
                if title_text:
                    chunks.append(("title", f"{prefix}{title_text}" if not label else title_text))
            elif field == "total":
                if defer_summary_line:
                    continue
                if total_line and not total_emitted:
                    chunks.append(("total", total_line))
                    total_emitted = True
            elif field == "subtotal":
                if subtotal_line and not subtotal_emitted:
                    chunks.append(("subtotal", subtotal_line))
                    subtotal_emitted = True
            elif field == "item_subtotal":
                if item_subtotal_line and not item_subtotal_emitted:
                    chunks.append(("item_subtotal", item_subtotal_line))
                    item_subtotal_emitted = True
            elif field == "shipping_subtotal":
                if shipping_subtotal_line and not shipping_subtotal_emitted:
                    chunks.append(("shipping_subtotal", shipping_subtotal_line))
                    shipping_subtotal_emitted = True

        has_label = any(f == "label" for f, _ in chunks)
        has_qty = any(f == "qty" for f, _ in chunks)
        if not has_label and label:
            chunks.insert(0, ("label", f"{prefix}{label}"))
        if not has_qty:
            pos = 1 if chunks and chunks[0][0] == "label" else 0
            chunks.insert(pos, ("qty", qty_line))
        if line_groups:
            _emit_with_line_groups(chunks)
        else:
            _emit_with_inline(chunks)

    if not line_groups and not location_emitted and summary_location_line:
        lines.insert(0, summary_location_line)

    if defer_summary_line:
        summary_parts: list[str] = []
        if total_line:
            summary_parts.append(total_line)
            total_emitted = True
        if summary_location_line:
            summary_parts.append(summary_location_line)
            location_emitted = True
        if summary_parts:
            lines.append(INLINE_LOCK_PREFIX + inline_sep.join(summary_parts))
    if total_line and not total_emitted:
        lines.append(total_line)
    if subtotal_line and not subtotal_emitted:
        lines.append(subtotal_line)
    if item_subtotal_line and not item_subtotal_emitted:
        lines.append(item_subtotal_line)
    if shipping_subtotal_line and not shipping_subtotal_emitted:
        lines.append(shipping_subtotal_line)

    manual_prefix = _auto_overlay_prefix_text(order)
    if manual_prefix:
        for idx, line in enumerate(lines):
            clean, _ = _strip_emph_markers(line)
            if clean.strip():
                if line.startswith(INLINE_LOCK_PREFIX):
                    locked = line[len(INLINE_LOCK_PREFIX):]
                    lines[idx] = INLINE_LOCK_PREFIX + f"{manual_prefix} {locked}"
                else:
                    lines[idx] = f"{manual_prefix} {line}"
                break

    return lines

COMPACT_PAIR_SEP = "  \u00b7  "


def build_compact_overlay_lines(
    order: dict[str, Any], item_rows: list[dict[str, str]], config: dict[str, Any]
) -> list[str]:
    """Build overlay lines in compact 2-per-line format for high-item-count labels.

    Items are paired on the same line with a middle-dot separator so that the
    overall line count is roughly halved compared to the normal format.
    """
    layout = config.get("print_layout", {})
    show_labels = layout.get("show_field_labels", True)
    inline_sep = str(layout.get("inline_separator", " | "))

    locations = [r.get("location", "").strip() for r in item_rows if r.get("location", "").strip()]
    uniq_locations: list[str] = []
    for loc in locations:
        if loc not in uniq_locations:
            uniq_locations.append(loc)

    items = order.get("items", []) or []
    use_numbering = len(items) > 1

    total = _selected_order_total(order, config)
    total_label = _selected_total_label(config)
    total_line = _money_line(total_label, total, show_labels)

    def _flag_on(value: Any, default: bool = True) -> bool:
        if value is None:
            return default
        s = str(value).strip().lower()
        if not s:
            return default
        return s not in ("0", "false", "off", "no")

    # Build one short string per item: "QTY 1 | 1) Label"
    item_strs: list[str] = []
    for idx, item in enumerate(items, start=1):
        title = (item.get("title") or "").strip()
        qty = int(item.get("quantity", 1) or 1)
        row = next((r for r in item_rows if _item_row_matches(r, item)), None)

        label = _append_item_variation(
            (
                (row or {}).get("custom_label", "").strip()
                or (row or {}).get("item_title", "").strip()
                or (item.get("item_sku") or "").strip()
                or (item.get("item_id") or "").strip()
                or title
            ),
            item,
        )

        prefix = f"{idx}) " if use_numbering else ""
        qty_part = f"*QTY {qty}*" if qty > 1 else f"QTY {qty}" if show_labels else f"x{qty}"
        item_str = f"{qty_part}{inline_sep}{prefix}{label}" if show_labels else f"{prefix}{label} x{qty}"
        item_strs.append(item_str)

    # Pair items 2-per-line
    lines: list[str] = []
    for i in range(0, len(item_strs), 2):
        if i + 1 < len(item_strs):
            lines.append(INLINE_LOCK_PREFIX + item_strs[i] + COMPACT_PAIR_SEP + item_strs[i + 1])
        else:
            lines.append(INLINE_LOCK_PREFIX + item_strs[i])

    # Summary line: TOTAL + LOC
    summary_parts: list[str] = []
    if total_line:
        summary_parts.append(total_line)
    if uniq_locations:
        loc_text = " | ".join(uniq_locations)
        summary_parts.append(("LOC " + loc_text) if show_labels else loc_text)
    if summary_parts:
        lines.append(INLINE_LOCK_PREFIX + inline_sep.join(summary_parts))

    manual_prefix = _auto_overlay_prefix_text(order)
    if manual_prefix and lines:
        first = lines[0]
        if first.startswith(INLINE_LOCK_PREFIX):
            locked = first[len(INLINE_LOCK_PREFIX):]
            lines[0] = INLINE_LOCK_PREFIX + f"{manual_prefix} {locked}"
        else:
            lines[0] = f"{manual_prefix} {first}"

    return lines


def create_summary_half_page(
    page_w: float,
    page_h: float,
    lines: list[str],
    config: dict[str, Any],
) -> bytes:
    """Render a configurable overflow-summary page.

    Used when large orders still overflow after the normal margin/spill layout.
    The output can be a half-page or full-page appendix depending on settings.
    """
    layout = config.get("print_layout", {})
    font_name = _resolve_font_name(layout)
    font_size = int(layout.get("summary_page_font_size", layout.get("backside_font_size", layout.get("font_size", 16))))
    line_spacing = int(layout.get("summary_page_line_spacing", layout.get("backside_line_spacing", layout.get("line_spacing", 20))))
    wrap_mode = str(layout.get("summary_page_wrap_mode", layout.get("wrap_mode", "word")))
    text_align = str(layout.get("summary_page_text_align", layout.get("text_align", "left")))
    orientation = str(layout.get("summary_page_orientation", "normal"))
    page_mode = str(layout.get("summary_page_mode", "half_page"))
    margin = float(layout.get("summary_page_margin", 24))

    render_h = page_h / 2.0 if page_mode == "half_page" else page_h
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, render_h))

    x = margin
    y = margin
    w = max(40.0, page_w - (2 * margin))
    h = max(40.0, render_h - (2 * margin))

    if orientation == "rotated_90":
        logical_line_width = h
        cap_by_height = max(1, int(w // max(1, line_spacing)))
    else:
        logical_line_width = w
        cap_by_height = max(1, int(h // max(1, line_spacing)))

    wrapped = _expand_lines(lines, font_name, font_size, logical_line_width, wrap_mode)
    c.setFont(font_name, font_size)

    if orientation == "rotated_90":
        idx = 0
        while idx < len(wrapped):
            c.saveState()
            c.translate(x + w, y)
            c.rotate(90)
            current_y = w - font_size
            lines_left = cap_by_height
            while idx < len(wrapped) and lines_left > 0:
                _draw_text_line_local(c, current_y, logical_line_width, wrapped[idx], text_align, font_name, font_size)
                current_y -= line_spacing
                idx += 1
                lines_left -= 1
            c.restoreState()
            if idx < len(wrapped):
                c.showPage()
                c.setFont(font_name, font_size)
    else:
        cur_y = y + h - font_size
        for idx, line in enumerate(wrapped):
            if idx and cur_y < y:
                c.showPage()
                c.setFont(font_name, font_size)
                cur_y = y + h - font_size
            _draw_text_line(c, x, cur_y, logical_line_width, line, text_align, font_name, font_size)
            cur_y -= line_spacing

    c.save()
    return buf.getvalue()

def create_overlay_pdf(
    page_w: float,
    page_h: float,
    lines: list[str],
    config: dict[str, Any],
    draw_rect: bool = False,
    region: str = "primary",
    preset_override: str | None = None,
) -> tuple[bytes, list[str]]:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))

    layout = config["print_layout"]
    font_name = _resolve_font_name(layout)
    font_size = int(layout.get("font_size", 16))
    line_spacing = int(layout.get("line_spacing", 20))
    wrap_mode = str(layout.get("wrap_mode", "truncate"))
    text_align = str(layout.get("text_align", "left"))
    orientation = str(layout.get("orientation_mode", "normal"))

    if orientation == "rotated_90":
        preset = preset_override or _resolve_preset(layout, region)
        x, y, w, h = _safe_rect(config, page_w, page_h, preset_override=preset)
    else:
        x, y, w, h = (
            _safe_rect(config, page_w, page_h, preset_override=preset_override)
            if region == "primary"
            else _secondary_rect(config, page_w, page_h)
        )

    if draw_rect:
        c.setStrokeColorRGB(1, 0, 0)
        c.rect(x, y, w, h, stroke=1, fill=0)

    if orientation == "rotated_90":
        logical_line_width = h
        # Prevent zero-line pages, which can stall pagination.
        cap_by_height = max(1, int(w // max(1, line_spacing)))
    else:
        logical_line_width = w
        cap_by_height = max(1, int(h // max(1, line_spacing)))

    max_lines_cfg = int(layout.get("max_lines", cap_by_height))
    max_lines = max(1, min(cap_by_height, max_lines_cfg))

    wrapped_lines = _expand_lines(lines, font_name, font_size, logical_line_width, wrap_mode)
    drawn = wrapped_lines[:max_lines]
    if region == "primary" and len(wrapped_lines) > len(drawn) and drawn:
        drawn[-1] = _with_continuation_notice(drawn[-1], font_name, font_size, logical_line_width)

    c.setFont(font_name, font_size)

    if orientation == "rotated_90":
        c.saveState()
        c.translate(x + w, y)
        c.rotate(90)
        current_y = w - font_size
        for line in drawn:
            _draw_text_line_local(c, current_y, logical_line_width, line, text_align, font_name, font_size)
            current_y -= line_spacing
            if current_y < 0:
                break
        c.restoreState()
    else:
        current_y = y + h - font_size
        for line in drawn:
            _draw_text_line(c, x, current_y, logical_line_width, line, text_align, font_name, font_size)
            current_y -= line_spacing
            if current_y < y:
                break

    c.save()
    return buf.getvalue(), wrapped_lines[len(drawn) :]


def create_info_panel_overlay_pdf(
    page_w: float,
    page_h: float,
    lines: list[str],
    config: dict[str, Any],
    draw_rect: bool = False,
) -> tuple[bytes, list[str]]:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))

    layout = config["print_layout"]
    font_name = _resolve_font_name(layout)
    font_size = int(layout.get("backside_font_size", layout.get("font_size", 16)))
    line_spacing = int(layout.get("backside_line_spacing", layout.get("line_spacing", 20)))
    wrap_mode = str(layout.get("wrap_mode", "truncate"))
    text_align = str(layout.get("text_align", "left"))
    edge_x = float(layout.get("edge_inset_x", 8))
    edge_y = float(layout.get("edge_inset_y", 24))
    page_mode = str(layout.get("page_mode", "half_sheet_top"))

    if page_mode == "half_sheet_top":
        region_y0 = 0.0
        region_y1 = page_h / 2.0
    elif page_mode == "half_sheet_bottom":
        region_y0 = page_h / 2.0
        region_y1 = page_h
    else:
        region_y0 = 0.0
        region_y1 = page_h

    x = edge_x
    y = region_y0 + edge_y
    w = max(80.0, page_w - (2 * edge_x))
    h = max(24.0, (region_y1 - region_y0) - (2 * edge_y))

    if draw_rect:
        c.setStrokeColorRGB(1, 0, 0)
        c.rect(x, y, w, h, stroke=1, fill=0)

    wrapped_lines = _expand_lines(lines, font_name, font_size, w, wrap_mode)
    c.setFont(font_name, font_size)
    cap_by_height = max(1, int(h // max(1, line_spacing)))
    drawn = wrapped_lines[:cap_by_height]
    current_y = y + h - font_size
    for line in drawn:
        _draw_text_line(c, x, current_y, w, line, text_align, font_name, font_size)
        current_y -= line_spacing
        if current_y < y:
            break

    c.save()
    return buf.getvalue(), wrapped_lines[len(drawn):]

def create_backside_pdf(page_w: float, page_h: float, lines: list[str], config: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))

    layout = config["print_layout"]
    font_name = _resolve_font_name(layout)
    font_size = int(layout.get("backside_font_size", layout.get("font_size", 16)))
    line_spacing = int(layout.get("backside_line_spacing", layout.get("line_spacing", 20)))
    wrap_mode = str(layout.get("wrap_mode", "word"))
    text_align = str(layout.get("text_align", "left"))
    orientation = str(layout.get("orientation_mode", "normal"))

    if orientation == "rotated_90":
        preset = _resolve_preset(layout, "primary")
        x, y, w, h = _safe_rect(config, page_w, page_h, preset_override=preset)
        logical_line_width = h
        cap_by_height = max(1, int(w // max(1, line_spacing)))
    else:
        x, y, w, h = _safe_rect(config, page_w, page_h)
        logical_line_width = w
        cap_by_height = max(1, int(h // max(1, line_spacing)))

    wrapped_lines = _expand_lines(lines, font_name, font_size, logical_line_width, wrap_mode)

    c.setFont(font_name, font_size)

    if orientation == "rotated_90":
        idx = 0
        while idx < len(wrapped_lines):
            c.saveState()
            c.translate(x + w, y)
            c.rotate(90)
            current_y = w - font_size
            lines_left = cap_by_height
            while idx < len(wrapped_lines) and lines_left > 0:
                _draw_text_line_local(c, current_y, logical_line_width, wrapped_lines[idx], text_align, font_name, font_size)
                current_y -= line_spacing
                idx += 1
                lines_left -= 1
            c.restoreState()
            if idx < len(wrapped_lines):
                c.showPage()
                c.setFont(font_name, font_size)
    else:
        cur_y = y + h - font_size
        for line in wrapped_lines:
            if cur_y < y:
                c.showPage()
                c.setFont(font_name, font_size)
                cur_y = y + h - font_size
            _draw_text_line(c, x, cur_y, logical_line_width, line, text_align, font_name, font_size)
            cur_y -= line_spacing

    c.save()
    return buf.getvalue()


def get_page_size(pdf_path: Path) -> tuple[float, float]:
    reader = PdfReader(str(pdf_path))
    page = reader.pages[0]
    return float(page.mediabox.width), float(page.mediabox.height)









