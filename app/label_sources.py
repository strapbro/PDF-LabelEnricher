"""Discover physical labels and adapt new Amazon inputs to the existing renderer.

Coordinates are in PyMuPDF's top-left system. A 6x4 landscape label is centered
in the selected Letter half-sheet, clear of the narrow enrichment margins, and
rotated counterclockwise from the original 4x6 image.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import fitz

from .platform_detector import detect_platform_from_path, parse_order_id_from_filename
from .overlay_renderer import side_margin_content_bounds
from .utils import atomic_write_json, sanitize_filename

LETTER_SIZE = (612, 792)
LABEL_SIZE = (288, 432)
LETTER_LABEL_RECT = (90, 54, 522, 342)
LEGACY_LETTER_LABEL_RECT = (18, 28.8, 450, 316.8)
RUNTIME_DIRS = {"_split_pages", "_label_sources"}
SUMMARY_TITLE = "list of orders with successful label purchase"


def natural_key(value: Any) -> list[tuple[int, Any]]:
    return [(1, int(p)) if p.isdigit() else (0, p.lower())
            for p in re.split(r"(\d+)", str(value).replace("\\", "/")) if p]


def staged_files(root: Path) -> list[Path]:
    return sorted((p for p in root.rglob("*") if p.is_file()
                   and not RUNTIME_DIRS.intersection(p.relative_to(root).parts)), key=natural_key)


def extract_archives(root: Path) -> None:
    """Refresh extraction and recursively expand ZIPs without flattening members."""
    root.mkdir(parents=True, exist_ok=True)
    workspace = root / "_unzipped"
    shutil.rmtree(workspace, ignore_errors=True)
    total_bytes = 0
    total_members = 0

    def unpack(archive: Path, destination: Path, depth: int) -> None:
        nonlocal total_bytes, total_members
        if depth > 8:
            raise ValueError("Label ZIP nesting exceeds 8 levels.")
        destination.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive) as source:
            for member in sorted(source.infolist(), key=lambda m: natural_key(m.filename)):
                if member.is_dir():
                    continue
                relative = PurePosixPath(member.filename.replace("\\", "/"))
                if relative.is_absolute() or ".." in relative.parts or any(":" in p for p in relative.parts):
                    raise ValueError("Label ZIP contains an unsafe member path.")
                total_bytes += member.file_size
                total_members += 1
                if total_bytes > 1024 * 1024 * 1024 or total_members > 10000:
                    raise ValueError("Label ZIP exceeds the extraction limit.")
                target = destination.joinpath(*relative.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.open(member) as incoming, target.open("wb") as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                if target.suffix.lower() == ".zip":
                    unpack(target, target.parent / (target.stem + "__contents"), depth + 1)

    for archive in sorted(root.glob("*"), key=natural_key):
        if archive.is_file() and archive.suffix.lower() == ".zip":
            unpack(archive, workspace / archive.stem, 0)


def is_4x6(rect: fitz.Rect) -> bool:
    width, height = sorted((rect.width, rect.height))
    return abs(width - 288) <= 2 and abs(height - 432) <= 2


def label_rect(config: dict[str, Any]) -> fitz.Rect:
    normalization = config.get("input_normalization", {})
    rect = fitz.Rect(normalization.get("amazon_4x6_letter_rect", LETTER_LABEL_RECT))
    if abs(rect.width - 432) > .01 or abs(rect.height - 288) > .01:
        raise ValueError("Amazon label placement must remain exactly 6x4 inches.")
    if config.get("print_layout", {}).get("page_mode") == "half_sheet_bottom":
        rect += (0, 396, 0, 396)
    if not fitz.Rect(0, 0, *LETTER_SIZE).contains(rect):
        raise ValueError("Amazon label placement extends outside the Letter page.")
    return rect


def normalize_page(source: fitz.Document, page_number: int, destination: Path,
                   config: dict[str, Any]) -> None:
    """Place a complete 4x6 page at physical size, retaining its original content."""
    # show_pdf_page imports underlying PDF coordinates, not the /Rotate view.
    # Bake that view into the in-memory page first to avoid clipping landscape
    # inputs. This retains vector/image content and never rewrites the upload.
    if source[page_number].rotation:
        source[page_number].remove_rotation()
    with fitz.open() as output:
        page = output.new_page(width=612, height=792)
        rotation = 90 if source[page_number].rect.width < source[page_number].rect.height else 0
        page.show_pdf_page(label_rect(config), source, page_number, rotate=rotation)
        output.save(str(destination))


def normalize_png(source: Path, destination: Path, config: dict[str, Any]) -> None:
    # Explicit dimensions are authoritative; Amazon's 1216x1824 PNGs have no DPI.
    # Embedding the original image stream retains all barcode pixels.
    image = fitz.Pixmap(str(source))
    if abs(image.width / image.height - 2 / 3) > .015:
        raise ValueError(f"Expected a portrait 4x6 label image: {source.name}")
    with fitz.open() as image_pdf:
        image_pdf.new_page(width=288, height=432).insert_image(
            fitz.Rect(0, 0, 288, 432), filename=str(source))
        normalize_page(image_pdf, 0, destination, config)


def inset_letter_label(source: Path, destination: Path, config: dict[str, Any]) -> bool:
    """Nudge crowded Letter artwork without scaling or rewriting its pixels."""
    limits = side_margin_content_bounds(config, *LETTER_SIZE)
    if limits is None:
        return False
    with fitz.open(str(source)) as document:
        page = document[0]
        if len(document) != 1 or page.rotation or abs(page.rect.width - 612) > .1 or abs(page.rect.height - 792) > .1:
            return False
        boxes = [fitz.Rect(box) for kind, box, *_ in page.get_bboxlog() if kind != "ignore-text"]
        if not boxes:
            return False
        ink = fitz.Rect()
        for box in boxes:
            ink |= box
        # A full-page bitmap includes white paper. A small grayscale preview
        # finds its actual ink; this is a bounds check, never OCR or output data.
        if any(box.contains(page.rect) for box in boxes):
            pixmap = page.get_pixmap(matrix=fitz.Matrix(.5, .5), colorspace=fitz.csGRAY, alpha=False)
            dark = [i for i, value in enumerate(pixmap.samples) if value < 200]
            if not dark:
                return False
            xs = [i % pixmap.width for i in dark]
            ys = [i // pixmap.width for i in dark]
            ink = fitz.Rect(min(xs) * 2, min(ys) * 2, (max(xs) + 1) * 2, (max(ys) + 1) * 2)
        # Edge-touching or out-of-page ink may already be cropped upstream.
        # Never move it farther off-paper, or shrink an oversized label to fit.
        if not (page.rect + (2, 2, -2, -2)).contains(ink):
            return False
        minimum_shift, maximum_shift = limits[0] - ink.x0, limits[1] - ink.x1
        if minimum_shift > maximum_shift:
            return False
        shift = min(max(0.0, minimum_shift), maximum_shift)
        if abs(shift) < .01:
            return False
        destination.parent.mkdir(parents=True, exist_ok=True)
        with fitz.open() as output:
            output.new_page(width=612, height=792).show_pdf_page(
                page.rect + (shift, 0, shift, 0), document, 0)
            output.save(str(destination))
    return True


def restage_label_source(record: dict[str, Any], destination: Path, archive: Path,
                         config: dict[str, Any]) -> dict[str, Any]:
    """Rebuild 4x6 placement during replay without moving old Letter labels."""
    source = dict(record)
    if source.get("format") not in ("amazon_4x6_png", "4x6_pdf"):
        shutil.copy2(source["pdf"], destination)
        return source
    raw = (archive / source.get("original_source", "")).resolve()
    rebuilt = False
    if raw.is_relative_to(archive.resolve()) and raw.is_file():
        if raw.suffix.lower() == ".png":
            normalize_png(raw, destination, config)
            rebuilt = True
        elif raw.suffix.lower() == ".pdf":
            with fitz.open(str(raw)) as document:
                number = int(source.get("page", 1)) - 1
                if 0 <= number < len(document) and is_4x6(document[number].rect):
                    normalize_page(document, number, destination, config)
                    rebuilt = True
    if not rebuilt:
        # Later replays retain the canonical PDF rather than another copy of
        # the original download. Its recorded rectangle isolates the complete
        # label and prevents accumulated shifts or loss of native pixels.
        with fitz.open(str(source["pdf"])) as document, fitz.open() as output:
            clip = source.get("normalization_rect")
            if clip is None:
                images = document[0].get_images()
                if source.get("format") == "amazon_4x6_png" and len(images) == 1:
                    clip = document[0].get_image_rects(images[0][0])[0]
                else:
                    clip = fitz.Rect(LEGACY_LETTER_LABEL_RECT)
                    bounds = fitz.Rect()
                    for _, box, *_ in document[0].get_bboxlog():
                        bounds |= fitz.Rect(box)
                    if not bounds.is_empty and (bounds.y0 + bounds.y1) / 2 > 396:
                        clip += (0, 396, 0, 396)
            output.new_page(width=612, height=792).show_pdf_page(
                label_rect(config), document, 0, clip=fitz.Rect(clip))
            output.save(str(destination))
    source["normalization_rect"] = list(label_rect(config))
    return source


def _summary_continuation(text: str) -> bool:
    # Amazon's second summary page has no heading, only order IDs and bullets.
    remaining = re.sub(r"\d{3}-\d{7}-\d{7}", "", text)
    return bool(re.search(r"\d{3}-\d{7}-\d{7}", text)) and not re.sub(r"[\s\-•]+", "", remaining)


def prepare_label_sources(root: Path, files: list[Path], destination: Path,
                          config: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one PDF per label plus durable provenance; leave old inputs intact."""
    destination.mkdir(parents=True, exist_ok=True)
    replay_path = root / "_source_manifest.json"
    replay = json.loads(replay_path.read_text(encoding="utf-8")) if replay_path.exists() else {}
    pngs = [p for p in files if p.suffix.lower() == ".png" and parse_order_id_from_filename(p)]
    png_parents = {p.parent for p in pngs}
    candidates = [p for p in files if (p in pngs or p.suffix.lower() == ".pdf")
                  and "packing slip" not in p.name.lower()
                  and not (p.name.lower() == "0_mergedlabeldoc.pdf" and p.parent in png_parents)]
    counts: dict[str, int] = {}
    for source in candidates:
        counts[source.name.lower()] = counts.get(source.name.lower(), 0) + 1
    sources: list[dict[str, Any]] = []

    for source in sorted(candidates, key=natural_key):
        relative = source.relative_to(root).as_posix()
        platform = detect_platform_from_path(source)
        with source.open("rb") as stream:
            content_digest = hashlib.file_digest(stream, "sha256").hexdigest()
        identity = f"{relative}:{content_digest}"
        digest = hashlib.sha256(identity.encode()).hexdigest()[:12]
        base = sanitize_filename(source.stem)
        if source.suffix.lower() == ".png" or counts[source.name.lower()] > 1 or not parse_order_id_from_filename(source):
            base += "__s" + digest
        original = {"original_source": relative, "platform": platform,
                    "order_id": parse_order_id_from_filename(source)}
        if source.name in replay:
            original = dict(replay[source.name])
            original.pop("pdf", None)
            base = source.stem
        if source.suffix.lower() == ".png":
            out = destination / (base + ".pdf")
            normalize_png(source, out, config)
            sources.append({**original, "source_id": digest, "format": "amazon_4x6_png",
                            "normalization_rect": list(label_rect(config)), "pdf": str(out)})
            continue
        with fitz.open(str(source)) as document:
            in_summary = False
            for number, page in enumerate(document):
                text = re.sub(r"\s+", " ", page.get_text()).strip().lower()
                if SUMMARY_TITLE in text:
                    in_summary = True
                    continue
                if in_summary and (_summary_continuation(text) or (not text and not page.get_images())):
                    continue
                in_summary = False
                name = base + (f"__p{number + 1:03d}" if len(document) > 1 else "") + ".pdf"
                out = destination / name
                small = is_4x6(page.rect)
                if small:
                    normalize_page(document, number, out, config)
                elif len(document) == 1:
                    if source.resolve() != out.resolve():
                        shutil.copy2(source, out)
                else:
                    with fitz.open() as single:
                        single.insert_pdf(document, from_page=number, to_page=number)
                        single.save(str(out))
                source_id = original.get("source_id") or hashlib.sha256(f"{identity}:{number}".encode()).hexdigest()[:12]
                placement = {"normalization_rect": list(label_rect(config))} if small else {}
                sources.append({**original, **placement, "source_id": source_id,
                                "page": original.get("page", number + 1),
                                "format": original.get("format", "4x6_pdf" if small else "pdf"),
                                "pdf": str(out)})
    atomic_write_json(destination / "manifest.json", {Path(s["pdf"]).name: s for s in sources})
    return sources
