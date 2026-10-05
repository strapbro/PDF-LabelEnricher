from __future__ import annotations

import copy
import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fitz

from app.batch_manager import BatchManager
from app.label_matcher import match_label
from app.label_sources import extract_archives, prepare_label_sources, restage_label_source, staged_files, LETTER_LABEL_RECT, label_rect
from app.overlay_renderer import _safe_rect
from app.label_text_extractor import extract_label_signals, _recipient_address_from_lines, _extract_tracking
from app.settings_manager import SettingsManager, DEFAULT_CONFIG
from app import ui_server

ID1 = "111-1234567-1234567"
ID2 = "112-2345678-2345678"
ID3 = "113-3456789-3456789"
OCR_LINES = ["FROM: TEST SENDER", "123 OTHER ST", "OTHER CITY CA 90210",
             "SHIP JANE SAMPLE", "TO: 10 EXAMPLE AVE", "TEST CITY TX 75001-1234",
             "USPS TRACKING #", "9400 1234 5678 1234 5678 90"]


def label_pdf(size=(288, 432), labels=1, summaries=0, order_id="") -> bytes:
    with fitz.open() as doc:
        for _ in range(labels):
            page = doc.new_page(width=size[0], height=size[1])
            page.insert_text((30, 35), "USPS GROUND ADVANTAGE", fontsize=12)
            page.insert_text((30, 95), "SHIP TO:\nJANE SAMPLE\n10 EXAMPLE AVE\nTEST CITY TX 75001", fontsize=11)
            if order_id:
                page.insert_text((30, 175), "Order " + order_id, fontsize=10)
            for x in range(30, 240, 5):
                page.draw_rect(fitz.Rect(x, 240, x + 2, 290), color=None, fill=(0, 0, 0))
        for i in range(summaries):
            page = doc.new_page(width=size[0], height=size[1])
            if i == 0:
                page.insert_text((10, 20), "List of orders with successful label purchase", fontsize=7)
            page.insert_text((10, 40), ID1 + "\n" + ID2, fontsize=9)
        return doc.tobytes()


def label_png(width=1216) -> bytes:
    with fitz.open(stream=label_pdf(), filetype="pdf") as doc:
        return doc[0].get_pixmap(matrix=fitz.Matrix(width / 288, width / 288), alpha=False).tobytes("png")


def bulk_zip(ids=(ID1, ID2, ID3), nested=False) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for index, order_id in enumerate(ids, 1):
            archive.writestr(f"bulk/{index}_{order_id}.png", label_png())
        archive.writestr("bulk/0_MergedLabelDoc.pdf", label_pdf(labels=len(ids), summaries=2))
    if not nested:
        return stream.getvalue()
    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("inner/amznbulklabels.zip", stream.getvalue())
    return outer.getvalue()


def settings_for(root: Path) -> SettingsManager:
    settings = SettingsManager.__new__(SettingsManager)
    settings.base_dir = root
    settings.resource_dir = Path(__file__).resolve().parents[1]
    settings.config_path = root / "config.yaml"
    settings._config = copy.deepcopy(DEFAULT_CONFIG)
    settings._config["print_layout"].update({"orientation_mode": "rotated_90", "placement_preset": "left_margin",
        "rotated_primary_preset": "left_margin", "margin_box_width": 18, "font_size": 8, "line_spacing": 9,
        "overflow_mode": "secondary_margin"})
    settings.ensure_directories()
    return settings


def write_orders(path: Path, ids=(ID1, ID2, ID3)) -> None:
    headers = ["order-id", "sku", "product-name", "quantity-purchased", "item-price", "recipient-name", "ship-postal-code"]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        for i, order_id in enumerate(ids):
            writer.writerow(dict(zip(headers, [order_id, f"SKU-{i}", f"Synthetic item {i}", "1", "12.50", "Jane Sample", "75001"])))


def order(order_id=ID1, platform="amazon", name="Jane Sample", postal="75001") -> dict:
    return {"platform": platform, "order_id": order_id, "ship_name": name, "ship_postal": postal,
            "tracking_number": "", "items": [{"item_id": "SKU-0", "title": "Synthetic item", "quantity": 1, "line_total": 12.5}],
            "total_paid": 12.5}


class SourceNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def prepare(self):
        extract_archives(self.root)
        return prepare_label_sources(self.root, staged_files(self.root), self.root / "_label_sources", DEFAULT_CONFIG)

    def test_nested_zip_uses_pngs_without_duplicate_merged_pages(self):
        (self.root / "drive.zip").write_bytes(bulk_zip(nested=True))
        sources = self.prepare()
        self.assertEqual([s["order_id"] for s in sources], [ID1, ID2, ID3])
        self.assertEqual(len(sources), 3)
        for source in sources:
            with fitz.open(source["pdf"]) as doc:
                self.assertEqual(tuple(doc[0].rect), (0, 0, 612, 792))
                self.assertEqual(len(doc), 1)

    def test_outer_zip_mixes_digit_and_letter_filenames(self):
        with zipfile.ZipFile(self.root / "drive.zip", "w") as archive:
            archive.writestr("123-orders.txt", "order-id\tsku\tquantity-purchased")
            archive.writestr("a9f11ee3.pdf", label_pdf(size=(612, 792)))
            archive.writestr("9d09004e.pdf", label_pdf(size=(612, 792)))
            archive.writestr("bulk.zip", bulk_zip(ids=(ID1,)))
        self.assertEqual(len(self.prepare()), 3)

    def test_png_authority_does_not_read_corrupt_companion(self):
        (self.root / f"1_{ID1}.png").write_bytes(label_png())
        (self.root / "0_MergedLabelDoc.pdf").write_bytes(b"not a PDF")
        self.assertEqual(len(self.prepare()), 1)

    def test_native_pixels_and_true_physical_size_are_preserved(self):
        for width in (1200, 1216):
            source = self.root / f"{width}_{ID1}.png"
            source.write_bytes(label_png(width))
            original = fitz.Pixmap(str(source))
            sources = prepare_label_sources(self.root, [source], self.root / "_label_sources", DEFAULT_CONFIG)
            with fitz.open(sources[0]["pdf"]) as doc:
                image = doc[0].get_images()[0]
                embedded = fitz.Pixmap(doc, image[0])
                self.assertEqual(embedded.samples, original.samples)
                self.assertEqual((embedded.width, embedded.height), (original.width, original.height))
                rectangle = doc[0].get_image_rects(image[0])[0]
                self.assertAlmostEqual(rectangle.width, 432, places=3)
                self.assertAlmostEqual(rectangle.height, 288, places=3)
                for got, wanted in zip(rectangle, LETTER_LABEL_RECT):
                    self.assertAlmostEqual(got, wanted, places=3)

    def test_numeric_order_is_preserved_before_output_sorting(self):
        for index, oid in ((10, ID3), (2, ID2), (1, ID1)):
            (self.root / f"{index}_{oid}.png").write_bytes(label_png())
        self.assertEqual([s["order_id"] for s in self.prepare()], [ID1, ID2, ID3])

    def test_4x6_label_is_centered_clear_of_enrichment_margins(self):
        config = settings_for(self.root)._config
        config["print_layout"].update({"edge_inset_x": 10, "edge_inset_y": 8,
            "margin_box_height": 2000, "rotated_secondary_preset": "right_margin"})
        for mode, center_y in (("half_sheet_top", 198), ("half_sheet_bottom", 594)):
            config["print_layout"]["page_mode"] = mode
            rect = label_rect(config)
            self.assertAlmostEqual((rect.x0 + rect.x1) / 2, 306)
            self.assertAlmostEqual((rect.y0 + rect.y1) / 2, center_y)
            self.assertEqual((rect.width, rect.height), (432, 288))
            for preset in ("left_margin", "right_margin"):
                x, y, w, h = _safe_rect(config, 612, 792, preset)
                margin = fitz.Rect(x, 792 - y - h, x + w, 792 - y)
                self.assertFalse(rect.intersects(margin))
                self.assertGreaterEqual(max(margin.x0 - rect.x1, rect.x0 - margin.x1), 6)

    def test_merged_pdf_filters_summary_and_headingless_continuation(self):
        (self.root / "merged.pdf").write_bytes(label_pdf(labels=2, summaries=2))
        sources = self.prepare()
        self.assertEqual(len(sources), 2)
        self.assertEqual([s["page"] for s in sources], [1, 2])

    def test_letter_pdf_is_passed_through_byte_for_byte(self):
        data = label_pdf(size=(612, 792))
        (self.root / f"1_{ID1}.pdf").write_bytes(data)
        self.assertEqual(Path(self.prepare()[0]["pdf"]).read_bytes(), data)

    def test_rotated_4x6_pdf_is_supported(self):
        for rotation in (0, 90, 180, 270):
            with self.subTest(rotation=rotation):
                with fitz.open(stream=label_pdf(), filetype="pdf") as doc:
                    doc[0].set_rotation(rotation)
                    source = self.root / "rotated.pdf"
                    doc.save(str(source))
                sources = prepare_label_sources(self.root, [source], self.root / "_label_sources", DEFAULT_CONFIG)
                with fitz.open(sources[0]["pdf"]) as doc:
                    self.assertEqual((doc[0].rect.width, doc[0].rect.height), (612, 792))
                    self.assertIn("JANE SAMPLE", doc[0].get_text())
                    safe = fitz.Rect(LETTER_LABEL_RECT) + (-.01, -.01, .01, .01)
                    self.assertTrue(all(safe.contains(draw["rect"]) for draw in doc[0].get_drawings()))
                    span = doc[0].get_text("dict")["blocks"][0]["lines"][0]["spans"][0]
                    self.assertAlmostEqual(span["size"], 12, places=3)

    def test_same_merged_basename_in_two_archives_has_distinct_ids(self):
        for name in ("one", "two"):
            with zipfile.ZipFile(self.root / (name + ".zip"), "w") as archive:
                archive.writestr("0_MergedLabelDoc.pdf", label_pdf(labels=2))
        sources = self.prepare()
        self.assertEqual(len({s["pdf"] for s in sources}), 4)
        self.assertEqual(len({s["source_id"] for s in sources}), 4)

    def test_4x6_pdf_replay_preserves_vector_barcodes_and_scale(self):
        original = self.root / "label.pdf"
        original.write_bytes(label_pdf())
        config = copy.deepcopy(DEFAULT_CONFIG)
        config["input_normalization"] = {"amazon_4x6_letter_rect": [18, 28.8, 450, 316.8]}
        source = prepare_label_sources(self.root, [original], self.root / "_label_sources", config)[0]
        source.pop("normalization_rect")  # Legacy PDF with no original download available.
        config.pop("input_normalization")
        for mode in ("half_sheet_bottom", "half_sheet_top"):
            config["print_layout"]["page_mode"] = mode
            destination = self.root / (mode + ".pdf")
            source = restage_label_source(source, destination, self.root / "missing_archive", config)
            source["pdf"] = str(destination)
            with fitz.open(destination) as document:
                self.assertIn("JANE SAMPLE", document[0].get_text())
                safe = label_rect(config) + (-.01, -.01, .01, .01)
                self.assertTrue(all(safe.contains(d["rect"]) for d in document[0].get_drawings()))
                spans = [s for b in document[0].get_text("dict")["blocks"] if b["type"] == 0
                         for line in b["lines"] for s in line["spans"]]
                self.assertAlmostEqual(spans[0]["size"], 12, places=3)

    def test_new_download_with_same_filename_has_distinct_source_identity(self):
        source = self.root / "0_MergedLabelDoc.pdf"
        source.write_bytes(label_pdf(order_id=ID1))
        first = prepare_label_sources(self.root, [source], self.root / "_label_sources", DEFAULT_CONFIG)[0]
        source.write_bytes(label_pdf(order_id=ID2))
        second = prepare_label_sources(self.root, [source], self.root / "_label_sources", DEFAULT_CONFIG)[0]
        self.assertNotEqual(first["source_id"], second["source_id"])
        self.assertNotEqual(first["pdf"], second["pdf"])

    def test_fresh_extraction_removes_stale_archive_members(self):
        archive = self.root / "bulk.zip"
        archive.write_bytes(bulk_zip())
        self.assertEqual(len(self.prepare()), 3)
        archive.write_bytes(bulk_zip(ids=(ID1,)))
        self.assertEqual(len(self.prepare()), 1)

    def test_unsafe_zip_path_is_rejected(self):
        with zipfile.ZipFile(self.root / "unsafe.zip", "w") as archive:
            archive.writestr("../outside.pdf", b"bad")
        with self.assertRaises(ValueError):
            extract_archives(self.root)
        self.assertFalse((self.root.parent / "outside.pdf").exists())


class LetterClearanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.settings = settings_for(self.root)
        self.settings._config["print_layout"].update({"edge_inset_x": 10, "edge_inset_y": 8,
            "margin_box_height": 2000, "rotated_secondary_preset": "right_margin"})
        self.manager = BatchManager(self.settings)
        self.output = self.root / "output"
        self.output.mkdir()

    def source(self, x=26, width=406, raster=False):
        path = self.root / f"{x}_{ID1}.pdf"
        with fitz.open() as doc:
            page = doc.new_page(width=612, height=792)
            page.insert_text((x + 10, 65), "USPS GROUND ADVANTAGE" if width > 200 else "LABEL", fontsize=12)
            for offset in range(0, int(width), 5):
                page.draw_rect(fitz.Rect(x + offset, 160, x + offset + 2, 210), color=None, fill=(0, 0, 0))
            if raster:
                image = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False).tobytes("png")
                with fitz.open() as image_pdf:
                    image_pdf.new_page(width=612, height=792).insert_image(fitz.Rect(0, 0, 612, 792), stream=image)
                    image_pdf.save(str(path))
            else:
                doc.save(str(path))
        return path

    def test_crowded_vector_letter_moves_without_scaling(self):
        source = self.source()
        with patch("app.label_text_extractor._ocr_windows_image_lines") as ocr:
            result = self.manager._normalize_label_source(source, order(), self.output)
        ocr.assert_not_called()
        with fitz.open(result) as doc:
            barcode = doc[0].get_drawings()[0]["rect"]
            self.assertGreaterEqual(barcode.x0, 46)
            self.assertEqual((barcode.width, barcode.height), (2, 50))
            span = doc[0].get_text("dict")["blocks"][0]["lines"][0]["spans"][0]
            self.assertAlmostEqual(span["size"], 12, places=3)

    def test_raster_letter_moves_without_resampling_native_pixels(self):
        source = self.source(raster=True)
        with fitz.open(source) as doc:
            original = fitz.Pixmap(doc, doc[0].get_images()[0][0])
        result = self.manager._normalize_label_source(source, order(), self.output)
        with fitz.open(result) as doc:
            image = doc[0].get_images()[0][0]
            embedded = fitz.Pixmap(doc, image)
            self.assertEqual(embedded.samples, original.samples)
            rectangle = doc[0].get_image_rects(image)[0]
            self.assertGreater(rectangle.x0, 0)
            self.assertEqual((rectangle.width, rectangle.height), (612, 792))

    def test_clear_clipped_and_oversized_sources_are_preserved(self):
        for x, width in ((100, 406), (488, 274), (3, 595)):
            with self.subTest(x=x):
                source = self.source(x=x, width=width)
                self.assertEqual(self.manager._normalize_label_source(source, order(), self.output), source)
        self.settings._config["print_layout"]["overlay_mode"] = "backside"
        source = self.source()
        self.assertEqual(self.manager._normalize_label_source(source, order(), self.output), source)

    def test_right_margin_moves_crowded_artwork_left_without_scaling(self):
        self.settings._config["print_layout"].update({"placement_preset": "right_margin",
            "rotated_primary_preset": "right_margin", "overflow_mode": "backside"})
        source = self.source(x=500, width=100)
        result = self.manager._normalize_label_source(source, order(), self.output)
        with fitz.open(result) as doc:
            barcode = doc[0].get_drawings()[-1]["rect"]
            self.assertLessEqual(barcode.x1, 566)
            self.assertEqual((barcode.width, barcode.height), (2, 50))

    def test_repeated_reprocessing_does_not_accumulate_the_letter_shift(self):
        source = self.source()
        original = source.read_bytes()
        shutil_path = self.settings.incoming_batch_folder / source.name
        shutil_path.write_bytes(original)
        write_orders(self.settings.incoming_batch_folder / "orders.txt", ids=(ID1,))
        result = self.manager.process_batch()
        positions = []
        for iteration in range(3):
            self.assertTrue(result["ok"], result)
            row = result["report"]["results"][0]
            self.assertEqual(Path(row["label_pdf"]).read_bytes(), original)
            with fitz.open(row["output_pdf"]) as doc:
                positions.append(tuple(doc[0].get_drawings()[0]["rect"]))
            if iteration < 2:
                result = self.manager.reprocess_latest_batch()
        self.assertGreaterEqual(positions[0][0], 46)
        self.assertTrue(all(position == positions[0] for position in positions))


class OcrMatchingTests(unittest.TestCase):
    def signals(self, name="Jane Sample", postal="75001"):
        return {"recipient_name": name, "ship_postal": postal, "text": "", "platform_hint": "unknown",
                "ocr_general_used": True, "recipient_block_verified": True}

    def test_ocr_uses_one_destination_block_and_ignores_sender(self):
        self.assertEqual(_recipient_address_from_lines(OCR_LINES), ("JANE SAMPLE", "75001-1234"))
        self.assertEqual(_recipient_address_from_lines(OCR_LINES[:3]), ("", ""))

    def test_unique_exact_recipient_matches(self):
        result = match_label(Path("uuid.pdf"), {ID1: order()}, "amazon", signals=self.signals())
        self.assertEqual(result["status"], "matched")

    def test_duplicate_recipient_is_unresolved(self):
        result = match_label(Path("uuid.pdf"), {ID1: order(), ID2: order(ID2)}, "amazon", signals=self.signals())
        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["reason"], "repeat_buyer_ambiguous")

    def test_fuzzy_ocr_and_whole_label_hits_do_not_auto_match(self):
        signals = self.signals(name="Jane Samp1e")
        signals["text"] = "Jane Sample 75001"
        self.assertEqual(match_label(Path("uuid.pdf"), {ID1: order()}, "amazon", signals=signals)["status"], "unresolved")
        signals = self.signals()
        signals["recipient_block_verified"] = False
        self.assertEqual(match_label(Path("uuid.pdf"), {ID1: order()}, "amazon", signals=signals)["status"], "unresolved")

    def test_unknown_platform_can_resolve_uniquely_in_mixed_reports(self):
        orders = {ID1: order(), "12-12345-12345": order("12-12345-12345", "ebay", "Alex Example")}
        result = match_label(Path("uuid.pdf"), orders, signals=self.signals())
        self.assertEqual(result["order"]["platform"], "amazon")
        orders["12-12345-12345"]["ship_name"] = "Jane Sample"
        self.assertEqual(match_label(Path("uuid.pdf"), orders, signals=self.signals())["status"], "unresolved")

    def test_filename_id_skips_ocr(self):
        with tempfile.TemporaryDirectory() as temporary:
            label = Path(temporary) / (ID1 + ".pdf")
            label.write_bytes(label_pdf())
            with patch("app.label_text_extractor._ocr_windows_image_lines") as ocr:
                extract_label_signals(label)
            ocr.assert_not_called()

    def test_orientation_fallback_and_cached_signals(self):
        with tempfile.TemporaryDirectory() as temporary:
            label = Path(temporary) / "uuid.pdf"
            with fitz.open(stream=label_pdf(), filetype="pdf") as source, fitz.open() as image_only:
                image_only.new_page(width=612, height=792).insert_image(fitz.Rect(20, 20, 308, 452), stream=source[0].get_pixmap().tobytes("png"))
                image_only.save(str(label))
            with patch("app.label_text_extractor._ocr_windows_image_lines", side_effect=[[], OCR_LINES]) as ocr:
                signals = extract_label_signals(label)
                result = match_label(label, {ID1: order()}, "amazon", signals=signals)
            self.assertEqual(ocr.call_count, 2)
            self.assertEqual(result["status"], "matched")
            self.assertEqual(signals["carrier"], "usps")
            self.assertTrue(signals["tracking_number"])

    def test_unavailable_ocr_stays_unresolved(self):
        signals = self.signals(name="", postal="")
        signals["ocr_status"] = "unavailable_or_unreadable"
        result = match_label(Path("uuid.pdf"), {ID1: order()}, "amazon", signals=signals)
        self.assertEqual(result["status"], "unresolved")
        self.assertEqual(result["reason"], "ocr_unavailable_or_unreadable")

    def test_tracking_is_extracted_from_token_groups(self):
        self.assertEqual(_extract_tracking("TRACKING # 9400 1234 5678 1234 5678 90 DELIVER"), "9400123456781234567890")
        self.assertEqual(_extract_tracking("UPS TRACKING: 1Z 999 AA1 01 2345 6784 END"), "1Z999AA10123456784")

    def test_explicit_order_id_precedes_tracking(self):
        orders = {ID1: order(), ID2: order(ID2)}
        orders[ID2]["tracking_number"] = "123456789012"
        signals = self.signals()
        signals.update(order_id_amazon=ID1, tracking_number="123456789012")
        self.assertEqual(match_label(Path("uuid.pdf"), orders, "amazon", signals=signals)["order"]["order_id"], ID1)

    def test_duplicate_tracking_does_not_guess_an_amazon_order(self):
        orders = {ID1: order(), ID2: order(ID2)}
        for record in orders.values():
            record["tracking_number"] = "123456789012"
        signals = self.signals()
        signals["tracking_number"] = "123456789012"
        self.assertEqual(match_label(Path("uuid.pdf"), orders, "amazon", signals=signals)["status"], "unresolved")


class BatchLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.settings = settings_for(Path(self.temporary.name))
        self.manager = BatchManager(self.settings)
        self.incoming = self.settings.incoming_batch_folder

    def stage(self, ids=(ID1, ID2, ID3)):
        (self.incoming / "amznbulklabels.zip").write_bytes(bulk_zip(ids=ids, nested=True))
        write_orders(self.incoming / "orders.txt", ids=ids)

    def test_batch_render_full_and_selected_reprocessing(self):
        self.stage()
        with patch("app.label_text_extractor._ocr_windows_image_lines") as ocr:
            result = self.manager.process_batch()
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["report"]["summary"]["matched"], 3)
            again = self.manager.reprocess_latest_batch()
            self.assertEqual(again["report"]["summary"]["matched"], 3)
            selected = self.manager.reprocess_selected_from_latest([ID2])
            self.assertEqual(len(selected["report"]["results"]), 1)
            self.assertEqual(selected["report"]["results"][0]["order_id"], ID2)
        ocr.assert_not_called()
        for row in result["report"]["results"]:
            self.assertTrue(Path(row["label_pdf"]).exists())
            with fitz.open(row["output_pdf"]) as doc:
                self.assertEqual((doc[0].rect.width, doc[0].rect.height), (612, 792))
        self.assertEqual(result["report"]["results"][1]["source_id"], selected["report"]["results"][0]["source_id"])

    def test_anonymous_order_is_not_filtered_out_of_report(self):
        self.stage()
        (self.incoming / "uuid.pdf").write_bytes(label_pdf(size=(612, 792)))
        self.manager._extract_zip_files()
        files = self.manager._all_batch_files()
        sources = self.manager._expand_multi_page_label_pdfs(self.manager._find_label_pdfs(files))
        write_orders(self.incoming / "orders.txt", ids=(ID1, ID2, ID3, "114-4567890-4567890"))
        orders, _ = self.manager._build_orders(files, sources)
        self.assertIn("114-4567890-4567890", orders)

    def test_reprocessing_rebuilds_4x6_placement_and_preserves_pixels(self):
        self.stage(ids=(ID1,))
        self.manager.settings._config["input_normalization"] = {
            "amazon_4x6_letter_rect": [18, 28.8, 450, 316.8]}
        first = self.manager.process_batch()
        manifest_path = Path(first["batch_dir"]) / "label_sources" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        for source in manifest.values():
            source.pop("normalization_rect", None)  # A batch from before this fix.
        manifest_path.write_text(json.dumps(manifest))
        original_pdf = first["report"]["results"][0]["label_pdf"]
        with fitz.open(original_pdf) as doc:
            original_pixels = fitz.Pixmap(doc, doc[0].get_images()[0][0]).samples
        # The first replay can rebuild from the archived PNG. Subsequent replays
        # must use saved geometry because only the canonical PDF is restaged.
        for index, mode in enumerate(("half_sheet_top", "half_sheet_bottom", "half_sheet_top")):
            self.manager.settings._config["input_normalization"]["amazon_4x6_letter_rect"] = [90, 54, 522, 342]
            self.manager.settings._config["print_layout"]["page_mode"] = mode
            result = (self.manager.reprocess_latest_batch() if index < 2
                      else self.manager.reprocess_selected_from_latest([ID1]))
            self.assertTrue(result["ok"], result)
            row = result["report"]["results"][0]
            self.assertEqual(row["source_id"], first["report"]["results"][0]["source_id"])
            with fitz.open(row["label_pdf"]) as doc:
                image = doc[0].get_images()[0][0]
                self.assertEqual(fitz.Pixmap(doc, image).samples, original_pixels)
                for actual, expected in zip(doc[0].get_image_rects(image)[0], label_rect(self.manager.settings.config)):
                    self.assertAlmostEqual(actual, expected, places=3)

    def test_unresolved_source_survives_later_batch_and_resolution(self):
        self.stage(ids=(ID1,))
        write_orders(self.incoming / "orders.txt", ids=(ID2,))
        first = self.manager.process_batch()
        queue_entry = self.manager._load_unresolved_queue()[0]
        source = self.manager._resolve_source_pdf_from_queue_entry(queue_entry)
        self.assertTrue(source.exists())
        self.stage(ids=(ID3,))
        self.manager.process_batch()
        restarted = BatchManager(self.settings)
        recovered = restarted._resolve_source_pdf_from_queue_entry(queue_entry)
        self.assertEqual(recovered, source)
        # Store manual candidates as the UI does, then exercise the public assignment path.
        queue = restarted._load_unresolved_queue()
        pending = next(q for q in queue if q["label_pdf"] == queue_entry["label_pdf"])
        pending["candidates"] = [{"order_id": ID1, "order": order(), "score": 1.0}]
        restarted._save_unresolved_queue(queue)
        resolved = restarted.resolve_unmatched(queue_entry["label_pdf"], ID1)
        self.assertTrue(resolved["ok"], resolved)
        manifest = json.loads((restarted._latest_batch_dir() / "label_sources" / "manifest.json").read_text())
        self.assertIn(recovered.name, manifest)
        self.assertTrue(first["ok"])

    def test_image_reprint_mixed_with_id_labels_keeps_report_and_cached_ocr(self):
        self.stage(ids=(ID1,))
        with fitz.open(stream=label_pdf(), filetype="pdf") as doc, fitz.open() as image:
            image.new_page(width=612, height=792).insert_image(fitz.Rect(20, 20, 308, 452), stream=doc[0].get_pixmap().tobytes("png"))
            image.save(str(self.incoming / "uuid.pdf"))
        write_orders(self.incoming / "orders.txt", ids=(ID1, ID2))
        path = self.incoming / "orders.txt"
        contents = path.read_text()
        # Only the UUID's report order has the OCR destination.
        contents = contents.replace(ID1 + "\tSKU-0\tSynthetic item 0\t1\t12.50\tJane Sample", ID1 + "\tSKU-0\tSynthetic item 0\t1\t12.50\tSam Other")
        path.write_text(contents)
        with patch("app.label_text_extractor._ocr_windows_image_lines", return_value=OCR_LINES) as ocr:
            result = self.manager.process_batch()
        self.assertEqual(result["report"]["summary"]["matched"], 2)
        self.assertEqual(ocr.call_count, 1)
        self.assertEqual({r["order_id"] for r in result["report"]["results"]}, {ID1, ID2})

    def test_repeat_unresolved_reprocess_does_not_duplicate_queue(self):
        self.stage(ids=(ID1,))
        write_orders(self.incoming / "orders.txt", ids=(ID2,))
        self.manager.process_batch()
        self.manager.reprocess_latest_batch()
        self.assertEqual(len(self.manager._load_unresolved_queue()), 1)

    def test_mixed_amazon_png_letter_and_ebay_keep_existing_paths(self):
        self.stage(ids=(ID1,))
        (self.incoming / (ID2 + ".pdf")).write_bytes(label_pdf(size=(612, 792), order_id=ID2))
        (self.incoming / "ebay-label-test.pdf").write_bytes(label_pdf(size=(612, 792), order_id="12-12345-12345"))
        orders = {ID1: order(), ID2: order(ID2), "12-12345-12345": order("12-12345-12345", "ebay")}
        with patch.object(self.manager, "_build_orders", return_value=(orders, {})):
            result = self.manager.process_batch()
        self.assertEqual(result["report"]["summary"]["matched"], 3)
        self.assertEqual(result["report"]["summary"]["errors"], 0)

    def test_manual_entry_and_preview_use_png_sources(self):
        self.stage(ids=(ID1,))
        manual = self.settings.manual_incoming_folder
        (manual / "amznbulklabels.zip").write_bytes(bulk_zip(ids=(ID1,), nested=True))
        with patch.object(ui_server, "settings", self.settings), patch.object(ui_server, "batch_manager", self.manager):
            self.assertEqual(len(ui_server._manual_label_pdf_paths()), 1)
            self.assertEqual(len(ui_server._available_label_pdfs(extract_zip=True)), 1)


if __name__ == "__main__":
    unittest.main()
