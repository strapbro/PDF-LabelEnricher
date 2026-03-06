from __future__ import annotations

from pathlib import Path
from typing import Any

from .overlay_renderer import create_overlay_pdf, get_page_size
from .pdf_merge import merge_overlay_on_first_page


def generate_preview(label_pdf: Path, lines: list[str], config: dict[str, Any], output_pdf: Path, draw_rect: bool = True) -> Path:
    w, h = get_page_size(label_pdf)
    overlay_bytes, _ = create_overlay_pdf(w, h, lines, config, draw_rect=draw_rect)
    merge_overlay_on_first_page(label_pdf, overlay_bytes, output_pdf)
    return output_pdf
