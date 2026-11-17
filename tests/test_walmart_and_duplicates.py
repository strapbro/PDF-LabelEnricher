from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
import zipfile

import fitz
from openpyxl import Workbook

from app.batch_manager import BatchManager
from app.item_db import ItemDB, ItemRecord
from app import ui_server
from app.label_text_extractor import _recipient_address_from_lines
from test_label_inputs import settings_for, bulk_zip, write_orders, ID1


def walmart_report(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Po Details"
    sheet.append(["PO#", "Order#", "Customer Name", "Zip", "SKU", "Item Description", "Qty",
                  "Item Cost", "Shipping Cost", "Tax", "Tracking Number", "Order Date", "Line#", "Status"])
    sheet.append(["129000000000001", "200000000000001", "Jane Sample", "01234", "WM-SKU", "Test item",
                  1, 12.5, 0, 1, "", "2026-11-16", 1, "LabelCreated"])
    sheet.append(["129000000000001", "200000000000001", "Jane Sample", "01234", "WM-SECOND", "Second item",
                  1, 5, 2, .5, "", "2026-11-16", 2, "LabelCreated"])
    workbook.save(path)


def shipping_pdf(path: Path, names=("Jane Sample",), tracking="9400123456781234567890") -> None:
    with fitz.open() as document:
        for name in names:
            page = document.new_page(width=288, height=432)
            page.insert_text((20, 40), "WALMART\nUSPS GROUND ADVANTAGE", fontsize=10)
            page.insert_text((20, 140), f"SHIP TO:\n{name}\n10 EXAMPLE AVE\nTEST CITY TX 01234", fontsize=10)
            page.insert_text((20, 340), tracking, fontsize=10)
        document.save(path)


class WalmartTests(unittest.TestCase):
    def test_walmart_can_merge_into_amazon_and_ebay_item(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = ItemDB(Path(temporary) / "items.csv", {})
            database.save_rows([
                ItemRecord(platform="ebay", ebay_item_number="123456789012", custom_label="Vacuum hose", location="A-01").as_dict(),
                ItemRecord(platform="amazon", amazon_sku="AMZ-HOSE", amazon_asin="B000TEST01", custom_label="Vacuum hose").as_dict(),
                ItemRecord(platform="walmart", walmart_sku="WM-HOSE", custom_label="Vacuum hose").as_dict(),
            ])
            rows = ui_server._rows_with_keys(database.load_rows())
            _, candidates = ui_server._build_items_assist(rows, [])
            self.assertIn(rows[0]["_row_key"], [candidate["row_key"] for candidate in candidates[rows[2]["_row_key"]]])
            result = database.update_rows_from_form({"source_page": "items", "row_0_row_key": rows[0]["_row_key"],
                "row_1_row_key": rows[1]["_row_key"], "row_1_merge_target_key": rows[0]["_row_key"],
                "row_2_row_key": rows[2]["_row_key"], "row_2_merge_target_key": rows[0]["_row_key"]})
            self.assertEqual(result["merged"], 2)
            self.assertEqual(len(database.load_rows()), 1)
            merged = database.load_rows()[0]
            self.assertEqual(merged["walmart_sku"], "WM-HOSE")
            for platform, identifier in (("ebay", "123456789012"), ("amazon", "AMZ-HOSE"),
                                         ("amazon", "B000TEST01"), ("walmart", "WM-HOSE")):
                self.assertEqual(database.index()[(platform, identifier)], merged)
            self.assertEqual(merged["custom_label"], "Vacuum hose")
            self.assertEqual(merged["location"], "A-01")
            self.assertNotIn(("ebay", "AMZ-HOSE"), database.index())
            self.assertNotIn(("ebay", "WM-HOSE"), database.index())
            database.ensure_item("walmart", "WM-HOSE", "New report title")
            self.assertEqual(len(database.load_rows()), 1)
            self.assertEqual(database.index()[("walmart", "WM-HOSE")]["custom_label"], "Vacuum hose")

    def test_amazon_and_walmart_share_a_manual_sku_suggestion(self):
        rows = ui_server._rows_with_keys([
            ItemRecord(platform="amazon", amazon_sku="SAME-SKU", custom_label="Internal label").as_dict(),
            ItemRecord(platform="walmart", walmart_sku="SAME-SKU", custom_label="SAME-SKU").as_dict(),
        ])
        _, candidates = ui_server._build_items_assist(rows, [])
        self.assertIn(rows[0]["_row_key"], [candidate["row_key"] for candidate in candidates[rows[1]["_row_key"]]])

    def test_walmart_linked_to_amazon_does_not_create_an_ebay_lookup(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = ItemDB(Path(temporary) / "items.csv", {})
            database.save_rows([ItemRecord(platform="both", amazon_sku="AMZ-SKU", walmart_sku="WM-SKU").as_dict()])
            self.assertNotIn(("ebay", "AMZ-SKU"), database.index())

    def test_linked_rows_with_different_walmart_ids_remain_distinct(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = ItemDB(Path(temporary) / "items.csv", {})
            database.save_rows([ItemRecord(platform="both", amazon_sku="AMZ-SKU", walmart_sku=sku).as_dict()
                                for sku in ("WM-FIRST", "WM-SECOND")])
            rows = ui_server._rows_with_keys(database.load_rows())
            self.assertEqual(len(rows), 2)
            self.assertNotEqual(rows[0]["_row_key"], rows[1]["_row_key"])
            self.assertTrue(all(database._row_identity(row) == row["_row_key"] for row in rows))

    def test_quantity_two_multiplies_item_cost_in_printed_subtotal(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            settings.config["print_layout"].update({"total_display_mode": "subtotal",
                "field_order": ["qty", "label", "subtotal"], "line_groups_csv": "qty,label;subtotal"})
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "Po Details"
            sheet.append(["PO#", "Order#", "Customer Name", "Zip", "SKU", "Qty", "Item Cost", "Shipping Cost", "Tax", "Item Description"])
            sheet.append(["129000000000001", "200000000000001", "Jane Sample", "01234", "WM-SKU", 2, 12.5, 3, 2, "Test item"])
            workbook.save(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "label.pdf")
            manager = BatchManager(settings)
            orders, _ = manager._build_orders(manager._all_batch_files(), [])
            order = orders["200000000000001"]
            self.assertEqual(order["item_subtotal_paid"], 25)
            self.assertEqual(order["shipping_subtotal_paid"], 3)
            self.assertEqual(order["subtotal_paid"], 28)
            self.assertEqual(order["total_paid"], 30)
            result = manager.process_batch()
            self.assertEqual(result["report"]["summary"]["matched"], 1)
            with fitz.open(result["report"]["results"][0]["output_pdf"]) as document:
                text = "\n".join(page.get_text() for page in document)
            self.assertIn("QTY 2", text)
            self.assertIn("SUBTOTAL $28.00", text)

    def test_report_import_groups_lines_and_preserves_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            settings = settings_for(root)
            path = settings.incoming_batch_folder / "PO_Data.xlsx"
            walmart_report(path)
            manager = BatchManager(settings)
            orders, _ = manager._build_orders([path], [])
            self.assertEqual(len(orders), 1)
            order = orders["200000000000001"]
            self.assertEqual(order["platform"], "walmart")
            self.assertEqual(order["ship_postal"], "01234")
            self.assertEqual([item["item_sku"] for item in order["items"]], ["WM-SKU", "WM-SECOND"])
            self.assertEqual(order["subtotal_paid"], 19.5)
            self.assertEqual(order["total_paid"], 21)

    def test_new_walmart_item_uses_sku_as_editable_label(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = ItemDB(Path(temporary) / "items.csv", {})
            row = database.ensure_item("walmart", "WM-SKU", "Long item title")
            self.assertEqual(row["custom_label"], "WM-SKU")
            self.assertEqual(row["needs_review"], "1")
            self.assertEqual(database.index()[("walmart", "WM-SKU")]["custom_label"], "WM-SKU")
            self.assertNotIn(("amazon", "WM-SKU"), database.index())

    def test_unanchored_walmart_usps_recipient_excludes_sender(self):
        lines = ["WALMART", "USPS GROUND ADVANTAGE", "SOCAL ESSENTIAL PARTS",
                 "20291 PASEO DEL PRADO,", "WALNUT CA, 91789 US",
                 "CARRIER -- LEAVE IF NO RESPONSE", "DENISE BAILEY", "112 STONEGATE VLG",
                 "QUAKERTOWN PA, 18951", "9200 1903 6307 2228 9506 08"]
        self.assertEqual(_recipient_address_from_lines(lines), ("DENISE BAILEY", "18951"))
        self.assertEqual(_recipient_address_from_lines(lines[:5]), ("", ""))

    def test_review_save_keeps_walmart_sku_and_custom_label(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = ItemDB(Path(temporary) / "items.csv", {})
            row = database.ensure_item("walmart", "WM-SKU", "Test item")
            database.update_rows_from_form({"source_page": "review", "row_0_row_key": database._row_identity(row),
                                            "row_0_custom_label": "Warehouse label", "row_0_walmart_sku": "WM-SKU"})
            saved = database.index()[("walmart", "WM-SKU")]
            self.assertEqual(saved["custom_label"], "Warehouse label")
            self.assertEqual(saved["needs_review"], "0")

    def test_export_with_incorrect_used_range_still_imports_all_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            path = settings.incoming_batch_folder / "PO_Data.xlsx"
            walmart_report(path)
            with zipfile.ZipFile(path) as archive:
                entries = {name: archive.read(name) for name in archive.namelist()}
            entries["xl/worksheets/sheet1.xml"] = entries["xl/worksheets/sheet1.xml"].replace(b'A1:N3', b'A1')
            with zipfile.ZipFile(path, "w") as archive:
                for name, data in entries.items():
                    archive.writestr(name, data)
            orders, _ = BatchManager(settings)._build_orders([path], [])
            self.assertEqual(len(orders["200000000000001"]["items"]), 2)


class DuplicateTests(unittest.TestCase):
    def test_amazon_pngs_in_separate_archives_are_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            write_orders(settings.incoming_batch_folder / "orders.txt", ids=(ID1,))
            for name in ("amznbulklabels.zip", "amznbulklabels-copy.zip"):
                (settings.incoming_batch_folder / name).write_bytes(bulk_zip(ids=(ID1,)))
            result = BatchManager(settings).process_batch()
            self.assertTrue(result["needs_duplicate_confirmation"])
            self.assertEqual(len(result["duplicates"]), 1)

    def test_duplicate_pages_require_confirmation_and_can_be_skipped(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "labels.pdf", names=("Jane Sample", "Jane Sample"))
            blocked = manager.process_batch()
            self.assertTrue(blocked.get("needs_duplicate_confirmation"))
            self.assertEqual(len(blocked["duplicates"]), 1)
            self.assertIsNone(manager._latest_batch_dir())
            result = manager.process_batch(duplicate_action="skip")
            self.assertTrue(result["ok"])
            self.assertEqual(result["report"]["summary"]["duplicates_skipped"], 1)
            self.assertEqual(len(result["report"]["results"]), 1)

    def test_include_repeated_pages_keeps_two_printed_labels(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "labels.pdf", names=("Jane Sample", "Jane Sample"))
            included = manager.process_batch(duplicate_action="include")
            self.assertEqual(included["report"]["summary"]["matched"], 2)
            combined = manager.combine_latest_output_pdfs()
            with fitz.open(combined["path"]) as document:
                self.assertEqual(len(document), 2)

    def test_renamed_file_warns_across_batches_and_override_works(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            report = settings.incoming_batch_folder / "PO_Data.xlsx"
            label = settings.incoming_batch_folder / "first.pdf"
            walmart_report(report)
            shipping_pdf(label)
            first = manager.process_batch()
            self.assertEqual(first["report"]["summary"]["matched"], 1)
            manager.clear_staged_files()
            walmart_report(report)
            shipping_pdf(settings.incoming_batch_folder / "renamed.pdf")
            blocked = manager.process_batch()
            self.assertTrue(blocked.get("needs_duplicate_confirmation"))
            self.assertEqual(blocked["duplicates"][0]["previous_file"], "first.pdf")
            self.assertTrue(blocked["duplicates"][0]["processed_at"])
            included = manager.process_batch(duplicate_action="include")
            self.assertEqual(included["report"]["summary"]["matched"], 1)

    def test_tracking_detects_different_artwork_and_skip_all_keeps_latest_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "first.pdf")
            first = manager.process_batch()
            manager.clear_staged_files()
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "new.pdf", names=("Different Name",))
            self.assertTrue(manager.process_batch().get("needs_duplicate_confirmation"))
            skipped = manager.process_batch(duplicate_action="skip")
            self.assertTrue(skipped["all_duplicates_skipped"])
            self.assertEqual(str(manager._latest_batch_dir()), first["batch_dir"])

    def test_history_expires_after_24_hours(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "label.pdf")
            first = manager.process_batch()
            path = Path(first["batch_dir"]) / "batch_report.json"
            report = json.loads(path.read_text())
            report["timestamp"] = (datetime.now() - timedelta(hours=25)).strftime("%Y%m%d_%H%M%S_%f")
            path.write_text(json.dumps(report))
            manager.clear_staged_files()
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            shipping_pdf(settings.incoming_batch_folder / "label.pdf")
            self.assertEqual(manager.process_batch()["report"]["summary"]["matched"], 1)


if __name__ == "__main__":
    unittest.main()
