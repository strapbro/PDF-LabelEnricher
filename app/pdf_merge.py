from __future__ import annotations

import io
from pathlib import Path

from pypdf import PdfReader, PdfWriter


def merge_overlay_on_first_page(original_pdf: Path, overlay_pdf_bytes: bytes, output_pdf: Path) -> None:
    merge_overlays_on_first_page(original_pdf, [overlay_pdf_bytes], output_pdf)


def merge_overlays_on_first_page(original_pdf: Path, overlay_pdfs: list[bytes], output_pdf: Path) -> None:
    reader = PdfReader(str(original_pdf))
    writer = PdfWriter()

    overlays = []
    for b in overlay_pdfs:
        try:
            ov = PdfReader(io.BytesIO(b))
            if ov.pages:
                overlays.append(ov.pages[0])
        except Exception:
            continue

    for idx, page in enumerate(reader.pages):
        if idx == 0:
            for ov_page in overlays:
                page.merge_page(ov_page)
        writer.add_page(page)

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with output_pdf.open("wb") as f:
        writer.write(f)


def append_backside_page(original_pdf: Path, backside_pdf_bytes: bytes, output_pdf: Path) -> None:
    reader = PdfReader(str(original_pdf))
    backside = PdfReader(io.BytesIO(backside_pdf_bytes))
    writer = PdfWriter()

    for page in reader.pages:
        writer.add_page(page)
    for page in backside.pages:
        writer.add_page(page)

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with output_pdf.open("wb") as f:
        writer.write(f)
