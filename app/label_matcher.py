from __future__ import annotations

from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from .label_text_extractor import extract_label_signals
from .platform_detector import parse_order_id_from_filename


def _score_name(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _norm_order_id(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def _effective_platform_hint(initial_hint: str, signals: dict[str, Any]) -> str:
    hint = (initial_hint or "").strip().lower()
    if hint in ("amazon", "ebay"):
        return hint

    sig_hint = str(signals.get("platform_hint", "") or "").strip().lower()
    if sig_hint in ("amazon", "ebay"):
        return sig_hint

    if signals.get("order_id_amazon"):
        return "amazon"
    if signals.get("order_id_ebay"):
        return "ebay"
    return ""


def best_candidates(label_pdf: Path, orders: dict[str, dict[str, Any]], platform_hint: str) -> list[dict[str, Any]]:
    signals = extract_label_signals(label_pdf)
    candidates: list[dict[str, Any]] = []
    filename_order_id = parse_order_id_from_filename(label_pdf)
    filename_order_norm = _norm_order_id(filename_order_id)
    effective_hint = _effective_platform_hint(platform_hint, signals)

    for order_id, rec in orders.items():
        rec_platform = str(rec.get("platform", "") or "").strip().lower()

        # Hard guard for mixed batches: once we have a platform hint, do not score
        # orders from the other platform.
        if effective_hint and rec_platform and rec_platform != effective_hint:
            continue

        score = 0.0
        reasons: list[str] = []
        order_norm = _norm_order_id(order_id)

        if effective_hint and rec_platform == effective_hint:
            score += 0.2
            reasons.append("platform")

        # Amazon order IDs are usually present in label filenames. Treat this as strong signal.
        if filename_order_norm and filename_order_norm == order_norm:
            score += 1.3
            reasons.append("filename_order_id")

        if rec.get("tracking_number") and signals.get("tracking_number") and rec["tracking_number"] == signals["tracking_number"]:
            score += 1.1
            reasons.append("tracking")

        if signals.get("order_id_amazon") and _norm_order_id(signals["order_id_amazon"]) == order_norm:
            score += 1.0
            reasons.append("label_order_id_amazon")

        if signals.get("order_id_ebay") and _norm_order_id(signals["order_id_ebay"]) == order_norm:
            score += 1.0
            reasons.append("label_order_id_ebay")

        if rec.get("ship_postal") and signals.get("ship_postal") and rec["ship_postal"][:5] == signals["ship_postal"][:5]:
            score += 0.45
            reasons.append("zip")

        name_score = _score_name(rec.get("ship_name", ""), signals.get("recipient_name", ""))
        score += 0.5 * name_score
        if name_score >= 0.72:
            reasons.append("recipient_name")

        if score > 0:
            candidates.append(
                {
                    "order_id": order_id,
                    "score": round(score, 3),
                    "reasons": reasons,
                    "order": rec,
                    "signals": signals,
                }
            )

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:3]


def match_label(label_pdf: Path, orders: dict[str, dict[str, Any]], platform_hint: str = "") -> dict[str, Any]:
    cands = best_candidates(label_pdf, orders, platform_hint)
    if not cands:
        return {"status": "unresolved", "reason": "no_candidates", "candidates": []}
    top = cands[0]

    # If filename order ID matched, accept immediately.
    if "filename_order_id" in top.get("reasons", []):
        return {
            "status": "matched",
            "confidence": top["score"],
            "method": ",".join(top["reasons"]) or "score",
            "order": top["order"],
            "candidates": cands,
        }

    # Explicit IDs/tracking are high confidence.
    if any(r in top.get("reasons", []) for r in ["tracking", "label_order_id_amazon", "label_order_id_ebay"]) and top["score"] >= 1.0:
        return {
            "status": "matched",
            "confidence": top["score"],
            "method": ",".join(top["reasons"]) or "id_or_tracking",
            "order": top["order"],
            "candidates": cands,
        }

    # Zip + name can be enough for eBay/carrier labels, but keep strict margin.
    if "zip" in top.get("reasons", []) and "recipient_name" in top.get("reasons", []):
        second = cands[1]["score"] if len(cands) > 1 else 0.0
        if top["score"] >= 0.84 and (top["score"] - second) >= 0.15:
            return {
                "status": "matched",
                "confidence": top["score"],
                "method": ",".join(top["reasons"]) or "zip+name",
                "order": top["order"],
                "candidates": cands,
            }

    return {
        "status": "unresolved",
        "reason": "ambiguous_or_low_confidence",
        "candidates": cands,
        "signals": cands[0].get("signals") if cands else {},
    }
