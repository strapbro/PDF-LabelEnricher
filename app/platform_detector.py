from __future__ import annotations

import re
from pathlib import Path


# Use plain pattern (no word boundaries) so names like "1_114-...pdf" match reliably.
AMAZON_ORDER_RE = re.compile(r"\d{3}-\d{7}-\d{7}")


def parse_order_id_from_filename(path: Path) -> str:
    m = AMAZON_ORDER_RE.search(path.name)
    return m.group(0) if m else ""


def detect_platform_from_path(path: Path) -> str:
    lower = path.name.lower()
    full = str(path).lower()
    if lower.endswith(".txt") and "order report" in lower:
        return "amazon"
    if lower.endswith(".csv") and "ordersreport" in lower:
        return "ebay"
    if lower.endswith(".zip") and "amznbulklabels" in lower:
        return "amazon"
    # Extracted PDFs may have random names but parent folder still carries zip stem.
    if "amznbulklabels" in full:
        return "amazon"
    if parse_order_id_from_filename(path):
        return "amazon"
    if "ebay" in lower:
        return "ebay"
    if "amazon" in lower or "amzn" in lower:
        return "amazon"
    return "unknown"






