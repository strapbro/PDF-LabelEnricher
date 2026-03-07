from __future__ import annotations

import csv
import json
import re
import shutil
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .utils import now_iso


ASIN_RE = re.compile(r"^B[0-9A-Z]{9}$", re.IGNORECASE)
EBAY_ID_RE = re.compile(r"\b(\d{10,14})\b")
AMZ_LINK_ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})", re.IGNORECASE)
AMZ_ASIN_PARAM_RE = re.compile(r"[?&]asin=([A-Z0-9]{10})", re.IGNORECASE)

FIELDS = [
    "platform",
    "ebay_item_number",
    "amazon_sku",
    "amazon_asin",
    "item_id",  # legacy compatibility column
    "item_title",
    "custom_label",
    "location",
    "linked_platform",
    "linked_id_type",
    "linked_id_value",
    "show_label",
    "show_qty",
    "show_total_paid",
    "show_title",
    "show_location",
    "needs_review",
    "needs_review_reason",
    "last_seen",
]


@dataclass
class ItemRecord:
    platform: str
    ebay_item_number: str = ""
    amazon_sku: str = ""
    amazon_asin: str = ""
    item_id: str = ""
    item_title: str = ""
    custom_label: str = ""
    location: str = ""
    linked_platform: str = ""
    linked_id_type: str = ""
    linked_id_value: str = ""
    show_label: int = 1
    show_qty: int = 1
    show_total_paid: int = 1
    show_title: int = 0
    show_location: int = 1
    needs_review: int = 0
    needs_review_reason: str = ""
    last_seen: str = ""

    def as_dict(self) -> dict[str, str]:
        return _normalize_row(
            {
                "platform": self.platform,
                "ebay_item_number": self.ebay_item_number,
                "amazon_sku": self.amazon_sku,
                "amazon_asin": self.amazon_asin,
                "item_id": self.item_id,
                "item_title": self.item_title,
                "custom_label": self.custom_label,
                "location": self.location,
                "linked_platform": self.linked_platform,
                "linked_id_type": self.linked_id_type,
                "linked_id_value": self.linked_id_value,
                "show_label": str(int(self.show_label)),
                "show_qty": str(int(self.show_qty)),
                "show_total_paid": str(int(self.show_total_paid)),
                "show_title": str(int(self.show_title)),
                "show_location": str(int(self.show_location)),
                "needs_review": str(int(self.needs_review)),
                "needs_review_reason": self.needs_review_reason,
                "last_seen": self.last_seen,
            }
        )


def _is_asin(value: str) -> bool:
    return bool(ASIN_RE.fullmatch((value or "").strip()))


def _platform_for_ids(ebay_item_number: str, amazon_sku: str, amazon_asin: str, fallback: str) -> str:
    has_ebay = bool(ebay_item_number.strip())
    has_amz = bool(amazon_sku.strip() or amazon_asin.strip())
    if has_ebay and has_amz:
        return "both"
    if has_ebay:
        return "ebay"
    if has_amz:
        return "amazon"
    return (fallback or "").strip().lower()


def _normalize_row(row: dict[str, str]) -> dict[str, str]:
    clean = {k: (row.get(k, "") or "").strip() for k in FIELDS}

    # migrate legacy item_id into platform columns if needed
    legacy_id = clean.get("item_id", "")
    p = (clean.get("platform", "") or "").strip().lower()
    if legacy_id:
        if p == "ebay" and not clean.get("ebay_item_number"):
            clean["ebay_item_number"] = legacy_id
        elif p == "amazon" and not clean.get("amazon_sku") and not clean.get("amazon_asin"):
            if _is_asin(legacy_id):
                clean["amazon_asin"] = legacy_id.upper()
            else:
                clean["amazon_sku"] = legacy_id

    clean["amazon_asin"] = clean.get("amazon_asin", "").upper()

    lp = (clean.get("linked_platform", "") or "").strip().lower()
    if lp not in ("", "amazon", "ebay"):
        lp = ""
    clean["linked_platform"] = lp

    lit = (clean.get("linked_id_type", "") or "").strip().lower()
    if lit not in ("", "amazon_asin", "amazon_sku", "ebay_item_number", "item_id"):
        lit = ""
    clean["linked_id_type"] = lit

    liv = (clean.get("linked_id_value", "") or "").strip()
    if lit == "amazon_asin":
        liv = liv.upper()
    clean["linked_id_value"] = liv
    clean["show_label"] = "1"
    clean["show_qty"] = "1"
    clean["show_total_paid"] = "1"
    clean["show_location"] = "1"
    clean["show_title"] = "1" if str(clean.get("show_title", "0")).strip() == "1" else "0"
    clean["needs_review_reason"] = (clean.get("needs_review_reason", "") or "").strip()
    clean["platform"] = _platform_for_ids(clean["ebay_item_number"], clean["amazon_sku"], clean["amazon_asin"], p)

    # keep legacy item_id populated for backward compatibility
    if clean["platform"] == "ebay":
        clean["item_id"] = clean.get("ebay_item_number", "")
    elif clean["platform"] == "amazon":
        clean["item_id"] = clean.get("amazon_sku", "") or clean.get("amazon_asin", "")
    elif clean["platform"] == "both":
        clean["item_id"] = clean.get("amazon_sku", "") or clean.get("ebay_item_number", "") or clean.get("amazon_asin", "")

    return clean


def _row_ids(row: dict[str, str], platform: str) -> list[str]:
    p = platform.lower()
    if p == "ebay":
        return [x for x in [row.get("ebay_item_number", "").strip()] if x]
    if p == "amazon":
        return [x for x in [row.get("amazon_sku", "").strip(), row.get("amazon_asin", "").strip()] if x]
    return []


def _row_supports_platform(row: dict[str, str], platform: str) -> bool:
    rp = (row.get("platform", "") or "").lower()
    return rp == platform.lower() or rp == "both"


def _merge_two_rows(base: dict[str, str], other: dict[str, str]) -> dict[str, str]:
    out = dict(base)

    # IDs: preserve both sets.
    for key in ["ebay_item_number", "amazon_sku", "amazon_asin"]:
        if not out.get(key) and other.get(key):
            out[key] = other[key]

    # Prefer populated business fields.
    for key in ["custom_label", "location", "linked_platform", "linked_id_type", "linked_id_value", "needs_review_reason"]:
        if not out.get(key) and other.get(key):
            out[key] = other[key]

    # Prefer Amazon title if either row has Amazon IDs.
    out_has_amz = bool(out.get("amazon_sku") or out.get("amazon_asin"))
    oth_has_amz = bool(other.get("amazon_sku") or other.get("amazon_asin"))
    if oth_has_amz and other.get("item_title"):
        out["item_title"] = other["item_title"]
    elif not out.get("item_title") and other.get("item_title"):
        out["item_title"] = other["item_title"]
    elif out_has_amz and out.get("item_title"):
        pass

    # Keep toggles stable: if either row had a field enabled, keep enabled.
    for key in ["show_label", "show_qty", "show_total_paid", "show_location"]:
        out[key] = "1"
    out["show_title"] = "1" if (out.get("show_title", "0") == "1" or other.get("show_title", "0") == "1") else "0"

    # Needs review: if either needs review, keep it.
    out["needs_review"] = "1" if (out.get("needs_review", "0") == "1" or other.get("needs_review", "0") == "1") else "0"

    # latest seen
    if other.get("last_seen") and (not out.get("last_seen") or other.get("last_seen") > out.get("last_seen")):
        out["last_seen"] = other["last_seen"]

    return _normalize_row(out)


def _merge_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    # Intentionally do NOT auto-merge rows by inferred IDs.
    # Cross-platform linking is manual-only via linked_* fields.
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for raw in rows:
        row = _normalize_row(raw)
        sig = (
            (row.get("platform", "") or "").strip().lower(),
            (row.get("ebay_item_number", "") or "").strip(),
            (row.get("amazon_sku", "") or "").strip(),
            (row.get("amazon_asin", "") or "").strip().upper(),
            (row.get("item_id", "") or "").strip(),
        )
        if sig in seen:
            continue
        seen.add(sig)
        out.append(row)
    return out


class ItemDB:
    def __init__(self, csv_path: Path, defaults: dict[str, bool]) -> None:
        self.csv_path = csv_path
        self.defaults = defaults
        if not self.csv_path.exists():
            self.save_rows([])

    def load_rows(self) -> list[dict[str, str]]:
        if not self.csv_path.exists():
            return []
        with self.csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = [_normalize_row(r) for r in reader]
        rows = [r for r in rows if r.get("item_id") or r.get("ebay_item_number") or r.get("amazon_sku") or r.get("amazon_asin")]
        return _merge_rows(rows)

    @property
    def backups_dir(self) -> Path:
        return self.csv_path.parent / "backups" / "items_csv"

    def _backup_current_csv(self) -> Path | None:
        if not self.csv_path.exists():
            return None
        try:
            text = self.csv_path.read_text(encoding="utf-8-sig", errors="ignore").strip()
        except Exception:
            text = ""
        if not text:
            return None

        self.backups_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = self.backups_dir / f"items_{stamp}.csv"
        try:
            shutil.copy2(self.csv_path, dst)
        except Exception:
            return None

        files = sorted(self.backups_dir.glob("items_*.csv"), key=lambda p: p.stat().st_mtime)
        cutoff = datetime.now() - timedelta(days=90)
        for p in files:
            try:
                if datetime.fromtimestamp(p.stat().st_mtime) < cutoff:
                    p.unlink(missing_ok=True)
            except Exception:
                pass
        files = sorted(self.backups_dir.glob("items_*.csv"), key=lambda p: p.stat().st_mtime)
        while len(files) > 200:
            old = files.pop(0)
            try:
                old.unlink(missing_ok=True)
            except Exception:
                pass
        return dst

    @property
    def changelog_path(self) -> Path:
        return self.csv_path.parent / "logs" / "items_changes.jsonl"

    @property
    def changelog_text_path(self) -> Path:
        return self.csv_path.parent / "logs" / "items_changes.log"

    def create_backup_now(self) -> Path | None:
        return self._backup_current_csv()

    def _row_identity(self, row: dict[str, str]) -> str:
        parts = [
            (row.get("platform", "") or "").strip().lower(),
            (row.get("ebay_item_number", "") or "").strip(),
            (row.get("amazon_sku", "") or "").strip(),
            (row.get("amazon_asin", "") or "").strip().upper(),
            (row.get("item_id", "") or "").strip(),
        ]
        return "|".join(parts)

    def _append_change_log(self, action: str, before_rows: list[dict[str, str]], after_rows: list[dict[str, str]]) -> None:
        try:
            before_map = {self._row_identity(r): r for r in before_rows}
            after_map = {self._row_identity(r): r for r in after_rows}

            added = [k for k in after_map.keys() if k not in before_map]
            removed = [k for k in before_map.keys() if k not in after_map]
            updated = [k for k in after_map.keys() if k in before_map and after_map[k] != before_map[k]]

            ts = now_iso()
            payload = {
                "ts": ts,
                "action": action,
                "before_count": len(before_rows),
                "after_count": len(after_rows),
                "added": len(added),
                "removed": len(removed),
                "updated": len(updated),
                "sample_added": added[:5],
                "sample_removed": removed[:5],
                "sample_updated": updated[:5],
            }

            self.changelog_path.parent.mkdir(parents=True, exist_ok=True)
            with self.changelog_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=True) + "\n")

            def _clean(v: str, limit: int = 90) -> str:
                s = (v or "").replace("\n", " ").replace("\r", " ").strip()
                if len(s) > limit:
                    return s[: limit - 3] + "..."
                return s

            def _row_desc(row: dict[str, str]) -> str:
                plat = _clean(row.get("platform", ""))
                ebay = _clean(row.get("ebay_item_number", ""))
                sku = _clean(row.get("amazon_sku", ""))
                asin = _clean(row.get("amazon_asin", ""))
                label = _clean(row.get("custom_label", ""))
                loc = _clean(row.get("location", ""))
                return f"platform={plat} | ebay={ebay or '-'} | sku={sku or '-'} | asin={asin or '-'} | label={label or '-'} | loc={loc or '-'}"

            max_rows = 12
            lines: list[str] = []
            lines.append(f"[{ts}] action={action} before={len(before_rows)} after={len(after_rows)} added={len(added)} removed={len(removed)} updated={len(updated)}")

            if added:
                lines.append("  Added rows:")
                for k in added[:max_rows]:
                    lines.append("    + " + _row_desc(after_map[k]))
                if len(added) > max_rows:
                    lines.append(f"    ... {len(added) - max_rows} more added row(s)")

            if removed:
                lines.append("  Removed rows:")
                for k in removed[:max_rows]:
                    lines.append("    - " + _row_desc(before_map[k]))
                if len(removed) > max_rows:
                    lines.append(f"    ... {len(removed) - max_rows} more removed row(s)")

            if updated:
                lines.append("  Updated rows:")
                inspect_fields = [
                    "custom_label",
                    "location",
                    "item_title",
                    "ebay_item_number",
                    "amazon_sku",
                    "amazon_asin",
                    "show_label",
                    "show_total_paid",
                    "show_title",
                    "show_location",
                    "needs_review",
                    "needs_review_reason",
                    "linked_platform",
                    "linked_id_type",
                    "linked_id_value",
                ]
                for k in updated[:max_rows]:
                    b = before_map[k]
                    a = after_map[k]
                    lines.append("    ~ " + _row_desc(a))
                    diff_count = 0
                    for fld in inspect_fields:
                        bv = _clean(b.get(fld, ""), 60)
                        av = _clean(a.get(fld, ""), 60)
                        if bv != av:
                            lines.append(f"      {fld}: '{bv}' -> '{av}'")
                            diff_count += 1
                            if diff_count >= 10:
                                lines.append("      ... more field changes")
                                break
                if len(updated) > max_rows:
                    lines.append(f"    ... {len(updated) - max_rows} more updated row(s)")

            lines.append("")
            with self.changelog_text_path.open("a", encoding="utf-8") as f:
                f.write("\n".join(lines))
        except Exception:
            pass

    def save_rows(self, rows: Iterable[dict[str, str]], action: str = "save") -> None:
        before_rows = self.load_rows()
        canonical = _merge_rows([_normalize_row(r) for r in rows])
        tmp = self.csv_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            for row in canonical:
                writer.writerow({k: row.get(k, "") for k in FIELDS})
        tmp.replace(self.csv_path)
        self._append_change_log(action, before_rows, canonical)

    def index(self) -> dict[tuple[str, str], dict[str, str]]:
        idx: dict[tuple[str, str], dict[str, str]] = {}
        for row in self.load_rows():
            for ident in _row_ids(row, "ebay"):
                idx[("ebay", ident)] = row
            for ident in _row_ids(row, "amazon"):
                idx[("amazon", ident)] = row
            # legacy mapping fallback (limit Amazon legacy matching to rows lacking explicit Amazon IDs).
            legacy = (row.get("item_id") or "").strip()
            if legacy:
                if row.get("platform") in ("ebay", "both"):
                    idx[("ebay", legacy)] = row

            # Explicit manual cross-platform link alias.
            lp = (row.get("linked_platform") or "").strip().lower()
            lv = (row.get("linked_id_value") or "").strip()
            if lp in ("amazon", "ebay") and lv:
                idx[(lp, lv.upper() if lp == "amazon" else lv)] = row
        return idx

    def _find_row(self, rows: list[dict[str, str]], platform: str, ident: str) -> dict[str, str] | None:
        p = platform.lower().strip()
        i = ident.strip()
        if not i:
            return None
        for row in rows:
            if not _row_supports_platform(row, p):
                continue
            if i in _row_ids(row, p):
                return row
            # Legacy fallback only for eBay lookups.
            if p == "ebay" and i == (row.get("item_id") or "").strip():
                return row
        return None

    def ensure_item(self, platform: str, item_id: str, title: str = "", item_asin: str = "") -> dict[str, str]:
        p = platform.lower().strip()
        iid = (item_id or "").strip()
        asin = (item_asin or "").strip().upper()
        rows = self.load_rows()

        found = self._find_row(rows, p, iid)
        if not found and asin:
            found = self._find_row(rows, p, asin)

        if found:
            if p == "ebay" and iid and not found.get("ebay_item_number"):
                found["ebay_item_number"] = iid
            if p == "amazon":
                if iid and not _is_asin(iid) and not found.get("amazon_sku"):
                    found["amazon_sku"] = iid
                if asin and not found.get("amazon_asin"):
                    found["amazon_asin"] = asin
            if title:
                if p == "amazon" or not found.get("item_title"):
                    found["item_title"] = title[:220].strip()
            found["last_seen"] = now_iso()
            found.update(_normalize_row(found))
            self.save_rows(rows)
            return found

        rec = ItemRecord(
            platform=p,
            ebay_item_number=iid if p == "ebay" else "",
            amazon_sku=iid if p == "amazon" and not _is_asin(iid) else "",
            amazon_asin=(asin or (iid if p == "amazon" and _is_asin(iid) else "")).upper(),
            item_id=iid,
            item_title=(title or "")[:220].strip(),
            custom_label=(title or "")[:80].strip(),
            location="",
            show_label=int(bool(self.defaults.get("show_label", True))),
            show_qty=1,
            show_total_paid=int(bool(self.defaults.get("show_total_paid", True))),
            show_title=int(bool(self.defaults.get("show_title", False))),
            show_location=int(bool(self.defaults.get("show_location", True))),
            needs_review=1,
            needs_review_reason="Auto-added from batch; verify label/location mapping.",
            last_seen=now_iso(),
        )
        row = rec.as_dict()
        rows.append(row)
        self.save_rows(rows)
        return row

    def update_rows_from_form(self, form: dict[str, str]) -> tuple[int, int]:
        rows = self.load_rows()
        toggles = ["show_title"]
        kept: list[dict[str, str]] = [dict(r) for r in rows]
        deleted = 0
        source_page = (form.get("source_page", "") or "").strip().lower()

        # Save by stable row identity (not positional index), so filtered/sorted UI views
        # cannot accidentally overwrite a different row.
        submitted_prefixes: list[str] = []
        for k in form.keys():
            if k.endswith("_row_key") and k.startswith("row_"):
                submitted_prefixes.append(k[: -len("_row_key")])

        row_by_key: dict[str, dict[str, str]] = {self._row_identity(r): r for r in kept}
        delete_keys: set[str] = set()

        if submitted_prefixes:
            for prefix in submitted_prefixes:
                row_key = (form.get(prefix + "_row_key", "") or "").strip()
                if not row_key:
                    continue
                row = row_by_key.get(row_key)
                if row is None:
                    continue
                if form.get(prefix + "_delete_row") == "on":
                    delete_keys.add(row_key)
                    continue

                row["ebay_item_number"] = form.get(prefix + "_ebay_item_number", row.get("ebay_item_number", "")).strip()
                row["amazon_sku"] = form.get(prefix + "_amazon_sku", row.get("amazon_sku", "")).strip()
                row["amazon_asin"] = form.get(prefix + "_amazon_asin", row.get("amazon_asin", "")).strip().upper()
                row["item_title"] = form.get(prefix + "_item_title", row.get("item_title", "")).strip()
                row["custom_label"] = form.get(prefix + "_custom_label", row.get("custom_label", "")).strip()
                row["location"] = form.get(prefix + "_location", row.get("location", "")).strip()
                row["linked_platform"] = form.get(prefix + "_linked_platform", row.get("linked_platform", "")).strip().lower()
                row["linked_id_type"] = form.get(prefix + "_linked_id_type", row.get("linked_id_type", "")).strip().lower()
                row["linked_id_value"] = form.get(prefix + "_linked_id_value", row.get("linked_id_value", "")).strip()
                row["needs_review_reason"] = form.get(prefix + "_needs_review_reason", row.get("needs_review_reason", "")).strip()
                row["show_label"] = "1"
                row["show_qty"] = "1"
                row["show_total_paid"] = "1"
                row["show_location"] = "1"
                if source_page == "review":
                    row["needs_review"] = "0"
                    row["needs_review_reason"] = ""
                for t in toggles:
                    row[t] = "1" if form.get(prefix + "_" + t) == "on" else "0"
                row.update(_normalize_row(row))

            if delete_keys:
                before = len(kept)
                kept = [r for r in kept if self._row_identity(r) not in delete_keys]
                deleted = before - len(kept)
        else:
            # Backward compatibility fallback for older forms that do not include row keys.
            fallback_kept: list[dict[str, str]] = []
            for i, row in enumerate(kept):
                prefix = f"row_{i}_"
                if form.get(prefix + "delete_row") == "on":
                    deleted += 1
                    continue
                row["ebay_item_number"] = form.get(prefix + "ebay_item_number", row.get("ebay_item_number", "")).strip()
                row["amazon_sku"] = form.get(prefix + "amazon_sku", row.get("amazon_sku", "")).strip()
                row["amazon_asin"] = form.get(prefix + "amazon_asin", row.get("amazon_asin", "")).strip().upper()
                row["item_title"] = form.get(prefix + "item_title", row.get("item_title", "")).strip()
                row["custom_label"] = form.get(prefix + "custom_label", row.get("custom_label", "")).strip()
                row["location"] = form.get(prefix + "location", row.get("location", "")).strip()
                row["linked_platform"] = form.get(prefix + "linked_platform", row.get("linked_platform", "")).strip().lower()
                row["linked_id_type"] = form.get(prefix + "linked_id_type", row.get("linked_id_type", "")).strip().lower()
                row["linked_id_value"] = form.get(prefix + "linked_id_value", row.get("linked_id_value", "")).strip()
                row["needs_review_reason"] = form.get(prefix + "needs_review_reason", row.get("needs_review_reason", "")).strip()
                row["show_label"] = "1"
                row["show_qty"] = "1"
                row["show_total_paid"] = "1"
                row["show_location"] = "1"
                if source_page == "review":
                    row["needs_review"] = "0"
                    row["needs_review_reason"] = ""
                for t in toggles:
                    row[t] = "1" if form.get(prefix + t) == "on" else "0"
                row.update(_normalize_row(row))
                fallback_kept.append(row)
            kept = fallback_kept
        self.save_rows(kept)
        return len(kept), deleted

    def _norm_key(self, key: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (key or "").strip().lower())

    def _extract_ebay_id(self, value: str) -> str:
        s = (value or "").strip()
        m = EBAY_ID_RE.search(s)
        return m.group(1) if m else ""

    def _extract_asin_any(self, raw_asin: str, link: str) -> str:
        s = (raw_asin or "").strip().upper()
        if _is_asin(s):
            return s
        lnk = (link or "").strip()
        m = AMZ_LINK_ASIN_RE.search(lnk)
        if m and _is_asin(m.group(1).upper()):
            return m.group(1).upper()
        m = AMZ_ASIN_PARAM_RE.search(lnk)
        if m and _is_asin(m.group(1).upper()):
            return m.group(1).upper()
        return ""

    def _pick_label_column(self, fieldnames: list[str], keymap: dict[str, str]) -> str | None:
        def pick(*aliases: str) -> str | None:
            for a in aliases:
                h = keymap.get(self._norm_key(a))
                if h is not None:
                    return h
            return None

        col = pick(
            "model",
            "model number",
            "part model number",
            "part number",
            "internal name/label",
            "internal label",
            "custom label",
            "internal sku",
            "sku",
            "item label",
        )
        if col is not None:
            return col

        if fieldnames:
            first = fieldnames[0]
            if first is not None:
                return first
        return None

    def sync_from_master_csv(self, master_csv_path: Path) -> int:
        rows = self.load_rows()
        changed = 0
        with master_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters="\t,;")
            except Exception:
                dialect = csv.excel_tab if ("\t" in sample and sample.count("\t") >= sample.count(",")) else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            fn = reader.fieldnames or []
            keymap = {self._norm_key(h or ""): (h or "") for h in fn}

            def pick(*aliases: str) -> str | None:
                for a in aliases:
                    h = keymap.get(self._norm_key(a))
                    if h is not None:
                        return h
                return None

            col_label = self._pick_label_column(fn, keymap)
            col_desc = pick("description", "item title", "title")
            col_location = pick("location", "picking location", "picking location w warehouse r warehouse rack quotes no rack in that position o office p packing room")
            if col_location is None:
                for nk, raw in keymap.items():
                    if nk.startswith("pickinglocation"):
                        col_location = raw
                        break
            col_ebay_num = pick("ebay item #", "ebay item number", "item number")
            col_ebay_link = pick("ebay link")
            col_asin = pick("amazon asin", "amz asin", "asin")
            col_amz_link = pick("amazon link")

            for src in reader:
                label = (src.get(col_label, "") if col_label is not None else "").strip()
                desc = (src.get(col_desc, "") if col_desc is not None else "").strip()
                location = (src.get(col_location, "") if col_location is not None else "").strip()
                ebay = (src.get(col_ebay_num, "") if col_ebay_num is not None else "").strip()
                if not ebay:
                    ebay = self._extract_ebay_id(src.get(col_ebay_link, "") if col_ebay_link is not None else "")
                asin = self._extract_asin_any(src.get(col_asin, "") if col_asin is not None else "", src.get(col_amz_link, "") if col_amz_link is not None else "")

                if not (ebay or asin):
                    continue

                # Conservative sync: do not auto-merge Amazon and eBay records into one row.
                # If both IDs exist in source row, update/create each platform row separately.
                def _apply_common_fields(target_row: dict[str, str]) -> bool:
                    before = dict(target_row)
                    if label:
                        target_row["custom_label"] = label
                    elif desc and not target_row.get("custom_label"):
                        target_row["custom_label"] = desc[:80]
                    if desc and not target_row.get("item_title"):
                        target_row["item_title"] = desc[:220]
                    if location:
                        target_row["location"] = location
                    target_row["needs_review"] = "0"
                    target_row["needs_review_reason"] = ""
                    target_row.update(_normalize_row(target_row))
                    return target_row != before

                if ebay:
                    ebay_target = self._find_row(rows, "ebay", ebay)
                    if ebay_target is None:
                        ebay_target = ItemRecord(
                            platform="ebay",
                            ebay_item_number=ebay,
                            item_id=ebay,
                            custom_label=label or desc,
                            item_title=desc,
                            location=location,
                            needs_review=0,
                            needs_review_reason="",
                        ).as_dict()
                        rows.append(ebay_target)
                        changed += 1
                    else:
                        if _apply_common_fields(ebay_target):
                            changed += 1

                if asin:
                    amz_target = self._find_row(rows, "amazon", asin)
                    if amz_target is None:
                        amz_target = ItemRecord(
                            platform="amazon",
                            amazon_asin=asin,
                            item_id=asin,
                            custom_label=label or desc,
                            item_title=desc,
                            location=location,
                            needs_review=0,
                            needs_review_reason="",
                        ).as_dict()
                        rows.append(amz_target)
                        changed += 1
                    else:
                        if _apply_common_fields(amz_target):
                            changed += 1
        self.save_rows(rows)
        return changed


    def _parse_master_csv_candidates(self, master_csv_path: Path) -> list[dict[str, str]]:
        groups: dict[str, dict[str, str]] = {}

        def _group_key(ebay: str, asin: str, label: str, row_idx: int) -> str:
            if ebay and asin:
                return f"pair:{ebay}|{asin}"
            if label:
                return f"label:{label.lower()}"
            if ebay:
                return f"ebay:{ebay}"
            if asin:
                return f"amazon:{asin}"
            return f"row:{row_idx}"

        def _upsert_group(key: str, ebay: str, asin: str, label: str, title: str, location: str) -> None:
            current = groups.get(
                key,
                {
                    "ebay_item_number": "",
                    "amazon_asin": "",
                    "custom_label": "",
                    "item_title": "",
                    "location": "",
                },
            )
            if ebay and not current["ebay_item_number"]:
                current["ebay_item_number"] = ebay
            if asin and not current["amazon_asin"]:
                current["amazon_asin"] = asin
            if label:
                current["custom_label"] = label
            if title and not current["item_title"]:
                current["item_title"] = title
            if location:
                current["location"] = location
            groups[key] = current

        with master_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters="\t,;")
            except Exception:
                dialect = csv.excel_tab if ("\t" in sample and sample.count("\t") >= sample.count(",")) else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            fn = reader.fieldnames or []
            keymap = {self._norm_key(h or ""): (h or "") for h in fn}

            def pick(*aliases: str) -> str | None:
                for a in aliases:
                    h = keymap.get(self._norm_key(a))
                    if h is not None:
                        return h
                return None

            col_label = self._pick_label_column(fn, keymap)
            col_desc = pick("description", "item title", "title")
            col_location = pick("location", "picking location", "picking location w warehouse r warehouse rack quotes no rack in that position o office p packing room")
            if col_location is None:
                for nk, raw in keymap.items():
                    if nk.startswith("pickinglocation"):
                        col_location = raw
                        break
            col_ebay_num = pick("ebay item #", "ebay item number", "item number")
            col_ebay_link = pick("ebay link")
            col_asin = pick("amazon asin", "amz asin", "asin")
            col_amz_link = pick("amazon link")

            for row_idx, src in enumerate(reader):
                label = (src.get(col_label, "") if col_label is not None else "").strip()
                desc = (src.get(col_desc, "") if col_desc is not None else "").strip()
                location = (src.get(col_location, "") if col_location is not None else "").strip()
                ebay = (src.get(col_ebay_num, "") if col_ebay_num is not None else "").strip()
                if not ebay:
                    ebay = self._extract_ebay_id(src.get(col_ebay_link, "") if col_ebay_link is not None else "")
                asin = self._extract_asin_any(src.get(col_asin, "") if col_asin is not None else "", src.get(col_amz_link, "") if col_amz_link is not None else "")
                if not (ebay or asin):
                    continue

                key = _group_key(ebay, asin.upper(), label, row_idx)
                _upsert_group(key, ebay, asin.upper(), label, desc, location)

        out: list[dict[str, str]] = []
        for g in groups.values():
            ebay = (g.get("ebay_item_number", "") or "").strip()
            asin = (g.get("amazon_asin", "") or "").strip().upper()
            platform = "both" if (ebay and asin) else ("ebay" if ebay else "amazon")
            ident = f"{ebay}|{asin}" if platform == "both" else (ebay or asin)
            out.append(
                {
                    "platform": platform,
                    "ident": ident,
                    "ebay_item_number": ebay,
                    "amazon_asin": asin,
                    "custom_label": (g.get("custom_label", "") or "").strip(),
                    "item_title": (g.get("item_title", "") or "").strip(),
                    "location": (g.get("location", "") or "").strip(),
                }
            )
        return out

    def _find_row_for_master_entity(
        self, rows: list[dict[str, str]], ebay_item_number: str = "", amazon_asin: str = "", custom_label: str = ""
    ) -> dict[str, str] | None:
        ebay = (ebay_item_number or "").strip()
        asin = (amazon_asin or "").strip().upper()
        label = (custom_label or "").strip().lower()

        if ebay:
            r = self._find_row(rows, "ebay", ebay)
            if r is not None:
                return r
        if asin:
            r = self._find_row(rows, "amazon", asin)
            if r is not None:
                return r

        if label:
            for row in rows:
                row_label = (row.get("custom_label", "") or "").strip().lower()
                if row_label and row_label == label:
                    return row
        return None
    def preview_sync_from_master_csv(self, master_csv_path: Path) -> dict[str, Any]:
        rows = self.load_rows()
        candidates = self._parse_master_csv_candidates(master_csv_path)
        review_reason = "Imported from master CSV; verify mapping and values."
        entries: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        for c in candidates:
            ebay = (c.get("ebay_item_number", "") or "").strip()
            asin = (c.get("amazon_asin", "") or "").strip().upper()
            platform = (c.get("platform", "") or "").strip().lower()
            ident = (c.get("ident", "") or "").strip()
            if not platform or not ident or not (ebay or asin):
                continue
            key = (platform, ebay, asin)
            if key in seen:
                continue
            seen.add(key)

            found = self._find_row_for_master_entity(rows, ebay, asin, str(c.get("custom_label", "") or ""))
            before = dict(found) if found else {}

            if found:
                after = dict(found)
            else:
                after = ItemRecord(
                    platform="both" if (ebay and asin) else ("ebay" if ebay else "amazon"),
                    ebay_item_number=ebay,
                    amazon_asin=asin,
                    item_id=ebay or asin,
                    needs_review=1,
                    needs_review_reason=review_reason,
                    last_seen=now_iso(),
                ).as_dict()

            if ebay:
                after["ebay_item_number"] = ebay
            if asin:
                after["amazon_asin"] = asin
            src_label = (c.get("custom_label", "") or "").strip()
            src_title = (c.get("item_title", "") or "").strip()
            src_location = (c.get("location", "") or "").strip()
            if src_label:
                after["custom_label"] = src_label
            elif src_title and not after.get("custom_label"):
                after["custom_label"] = src_title[:80]
            if src_title and not after.get("item_title"):
                after["item_title"] = src_title[:220]
            if src_location:
                after["location"] = src_location
            after["needs_review"] = "1"
            after["needs_review_reason"] = review_reason
            after["last_seen"] = now_iso()
            after = _normalize_row(after)

            changed_fields: list[str] = []
            for fld in ["platform", "ebay_item_number", "amazon_asin", "custom_label", "item_title", "location", "needs_review", "needs_review_reason", "last_seen"]:
                if (before.get(fld, "") if before else "") != (after.get(fld, "") or ""):
                    changed_fields.append(fld)
            action = "create" if not found else ("update" if changed_fields else "noop")

            entries.append(
                {
                    "platform": platform,
                    "ident": ident,
                    "action": action,
                    "changed_fields": changed_fields,
                    "existing_row_key": self._row_identity(found) if found else "",
                    "preview_label": after.get("custom_label", "") or "",
                    "preview_title": after.get("item_title", "") or "",
                    "preview_location": after.get("location", "") or "",
                    "apply_fields": {
                        "platform": after.get("platform", ""),
                        "ebay_item_number": after.get("ebay_item_number", "") or "",
                        "amazon_asin": after.get("amazon_asin", "") or "",
                        "custom_label": after.get("custom_label", "") or "",
                        "item_title": after.get("item_title", "") or "",
                        "location": after.get("location", "") or "",
                        "needs_review": "1",
                        "needs_review_reason": review_reason,
                        "last_seen": after.get("last_seen", "") or now_iso(),
                    },
                }
            )

        creates = sum(1 for e in entries if e["action"] == "create")
        updates = sum(1 for e in entries if e["action"] == "update")
        noops = sum(1 for e in entries if e["action"] == "noop")
        return {"entries": entries, "counts": {"total": len(entries), "create": creates, "update": updates, "noop": noops}}
    def apply_staged_sync(self, staged_entries: list[dict[str, Any]], only_add_new: bool = False) -> dict[str, int]:
        rows = self.load_rows()
        created = 0
        updated = 0
        skipped = 0

        for entry in staged_entries:
            fields = dict(entry.get("apply_fields") or {})
            ebay = str(fields.get("ebay_item_number", "") or "").strip()
            asin = str(fields.get("amazon_asin", "") or "").strip().upper()
            custom_label = str(fields.get("custom_label", "") or "").strip()
            if not (ebay or asin):
                skipped += 1
                continue
            found = self._find_row_for_master_entity(rows, ebay, asin, custom_label)
            if found is not None:
                if only_add_new:
                    skipped += 1
                    continue
                before = dict(found)
                if ebay:
                    found["ebay_item_number"] = ebay
                if asin:
                    found["amazon_asin"] = asin
                for k in ["custom_label", "item_title", "location", "needs_review", "needs_review_reason", "last_seen"]:
                    if k in fields and str(fields.get(k, "")).strip():
                        found[k] = str(fields[k]).strip()
                found.update(_normalize_row(found))
                if found != before:
                    updated += 1
                else:
                    skipped += 1
                continue

            rec = ItemRecord(
                platform="both" if (ebay and asin) else ("ebay" if ebay else "amazon"),
                ebay_item_number=ebay,
                amazon_asin=asin,
                item_id=ebay or asin,
                custom_label=custom_label,
                item_title=str(fields.get("item_title", "") or "").strip(),
                location=str(fields.get("location", "") or "").strip(),
                needs_review=1,
                needs_review_reason=str(fields.get("needs_review_reason", "") or "Imported from master CSV; verify mapping and values.").strip(),
                last_seen=str(fields.get("last_seen", "") or now_iso()).strip(),
            ).as_dict()
            rows.append(rec)
            created += 1

        if created or updated:
            self.save_rows(rows, action="sync_staged_apply")
        return {"created": created, "updated": updated, "skipped": skipped}
    def clear_needs_review(self) -> int:
        rows = self.load_rows()
        cleared = 0
        for row in rows:
            if str(row.get("needs_review", "0")).strip() == "1":
                row["needs_review"] = "0"
                row["needs_review_reason"] = ""
                cleared += 1
        if cleared:
            self.save_rows(rows)
        return cleared


























