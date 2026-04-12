from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.batch_manager import _candidate_tracking_merge_group
from app.item_db import ItemDB, ItemRecord
from app.label_matcher import match_label


class RepeatBuyerMatcherTests(unittest.TestCase):
    def test_repeat_buyer_requires_manual_resolution_without_hard_signal(self) -> None:
        orders = {
            "23-14469-50891": {
                "platform": "ebay",
                "order_id": "23-14469-50891",
                "ship_name": "Kevin Dugi",
                "ship_postal": "78160-6939",
                "tracking_number": "9400108106244016052188",
                "sale_date": "Apr-11-26",
                "sale_date_sort": "2026-04-11",
                "items": [{"title": "Hunter PCM-300"}],
            },
            "21-14413-53992": {
                "platform": "ebay",
                "order_id": "21-14413-53992",
                "ship_name": "Kevin Dugi",
                "ship_postal": "78160-6939",
                "tracking_number": "9400108106244970128073",
                "sale_date": "Mar-29-26",
                "sale_date_sort": "2026-03-29",
                "items": [{"title": "Hunter PCM-300"}],
            },
        }

        signals = {
            "recipient_name": "Kevin Dugi",
            "ship_postal": "78160-6939",
            "tracking_number": "",
            "platform_hint": "ebay",
            "text": "",
            "order_id_amazon": "",
            "order_id_ebay": "",
        }

        with patch("app.label_matcher.extract_label_signals", return_value=signals):
            result = match_label(Path("kevin-label.pdf"), orders, "ebay")

        self.assertEqual(result.get("status"), "unresolved")
        self.assertEqual(result.get("reason"), "repeat_buyer_ambiguous")
        self.assertEqual(
            [candidate.get("order_id") for candidate in result.get("candidates", [])],
            ["23-14469-50891", "21-14413-53992"],
        )

    def test_tracking_signal_still_auto_matches_repeat_buyer(self) -> None:
        orders = {
            "23-14469-50891": {
                "platform": "ebay",
                "order_id": "23-14469-50891",
                "ship_name": "Kevin Dugi",
                "ship_postal": "78160-6939",
                "tracking_number": "9400108106244016052188",
                "sale_date": "Apr-11-26",
                "sale_date_sort": "2026-04-11",
                "items": [{"title": "Hunter PCM-300"}],
            },
            "21-14413-53992": {
                "platform": "ebay",
                "order_id": "21-14413-53992",
                "ship_name": "Kevin Dugi",
                "ship_postal": "78160-6939",
                "tracking_number": "9400108106244970128073",
                "sale_date": "Mar-29-26",
                "sale_date_sort": "2026-03-29",
                "items": [{"title": "Hunter PCM-300"}],
            },
        }

        signals = {
            "recipient_name": "Kevin Dugi",
            "ship_postal": "78160-6939",
            "tracking_number": "9400108106244016052188",
            "platform_hint": "ebay",
            "text": "",
            "order_id_amazon": "",
            "order_id_ebay": "",
        }

        with patch("app.label_matcher.extract_label_signals", return_value=signals):
            result = match_label(Path("kevin-label.pdf"), orders, "ebay")

        self.assertEqual(result.get("status"), "matched")
        self.assertEqual(result.get("order", {}).get("order_id"), "23-14469-50891")


class CandidateTrackingFallbackTests(unittest.TestCase):
    def test_fallback_does_not_jump_to_older_multi_order_group(self) -> None:
        candidates = [
            {
                "order_id": "23-14469-50891",
                "score": 1.15,
                "order": {
                    "tracking_number": "9400108106244016052188",
                    "sale_date_sort": "2026-04-11",
                    "sale_date": "Apr-11-26",
                },
            },
            {
                "order_id": "21-14413-53992",
                "score": 1.15,
                "order": {
                    "tracking_number": "9400108106244970128073",
                    "sale_date_sort": "2026-03-29",
                    "sale_date": "Mar-29-26",
                },
            },
            {
                "order_id": "07-14396-26068",
                "score": 1.15,
                "order": {
                    "tracking_number": "9434608106244936859388",
                    "sale_date_sort": "2026-03-20",
                    "sale_date": "Mar-20-26",
                },
            },
            {
                "order_id": "03-14401-85043",
                "score": 1.15,
                "order": {
                    "tracking_number": "9434608106244936859388",
                    "sale_date_sort": "2026-03-20",
                    "sale_date": "Mar-20-26",
                },
            },
        ]

        group = _candidate_tracking_merge_group(candidates)

        self.assertEqual(group, [])

    def test_fallback_keeps_top_tracking_group_when_top_candidate_belongs_to_combined_shipment(self) -> None:
        candidates = [
            {
                "order_id": "A-NEW",
                "score": 1.15,
                "order": {
                    "tracking_number": "9400108106244016059999",
                    "sale_date_sort": "2026-04-12",
                    "sale_date": "Apr-12-26",
                },
            },
            {
                "order_id": "A-OLD",
                "score": 1.15,
                "order": {
                    "tracking_number": "9400108106244016059999",
                    "sale_date_sort": "2026-04-11",
                    "sale_date": "Apr-11-26",
                },
            },
            {
                "order_id": "B-OTHER",
                "score": 1.05,
                "order": {
                    "tracking_number": "9400108106244016051234",
                    "sale_date_sort": "2026-04-10",
                    "sale_date": "Apr-10-26",
                },
            },
        ]

        group = _candidate_tracking_merge_group(candidates)

        self.assertEqual([candidate["order_id"] for candidate in group], ["A-NEW", "A-OLD"])


class BulkMergeSaveTests(unittest.TestCase):
    def test_save_can_apply_multiple_merge_selections_at_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "items.csv"
            db = ItemDB(
                csv_path,
                {"show_label": True, "show_total_paid": True, "show_title": False, "show_location": True},
            )

            db.save_rows(
                [
                    ItemRecord(
                        platform="ebay",
                        ebay_item_number="256002900458",
                        item_id="256002900458",
                        custom_label="Controller Module",
                    ).as_dict(),
                    ItemRecord(
                        platform="amazon",
                        amazon_asin="B000TEST01",
                        item_id="B000TEST01",
                        item_title="Controller Module Title",
                        location="A-01",
                    ).as_dict(),
                    ItemRecord(
                        platform="amazon",
                        amazon_sku="SKU-2",
                        item_id="SKU-2",
                        location="B-02",
                    ).as_dict(),
                ]
            )

            rows = db.load_rows()
            target_key = db._row_identity(rows[0])
            second_key = db._row_identity(rows[1])
            third_key = db._row_identity(rows[2])

            result = db.update_rows_from_form(
                {
                    "source_page": "items",
                    "row_0_row_key": target_key,
                    "row_1_row_key": second_key,
                    "row_1_merge_target_key": target_key,
                    "row_2_row_key": third_key,
                    "row_2_merge_target_key": second_key,
                }
            )

            self.assertEqual(result["merged"], 2)
            self.assertEqual(result["skipped_merges"], 0)

            merged_rows = db.load_rows()
            self.assertEqual(len(merged_rows), 1)
            merged = merged_rows[0]
            self.assertEqual(merged["platform"], "both")
            self.assertEqual(merged["ebay_item_number"], "256002900458")
            self.assertEqual(merged["amazon_asin"], "B000TEST01")
            self.assertEqual(merged["amazon_sku"], "SKU-2")
            self.assertEqual(merged["custom_label"], "Controller Module")
            self.assertEqual(merged["item_title"], "Controller Module Title")
            self.assertEqual(merged["location"], "A-01")


class ItemsSearchTemplateTests(unittest.TestCase):
    def test_search_indexes_live_field_values_and_refreshes_after_helper_actions(self) -> None:
        template = (Path(__file__).resolve().parents[1] / "templates" / "items.html").read_text(encoding="utf-8")

        self.assertIn("row?.querySelectorAll('input, textarea, select').forEach((field) => {", template)
        self.assertIn("const value = 'value' in field ? String(field.value || '') : '';", template)
        self.assertIn("field.selectedOptions?.[0]?.textContent", template)
        self.assertRegex(
            template,
            re.compile(r"input\.value = '';\s+input\.focus\(\);\s+markDirty\(\);\s+applyFilter\(\);", re.S),
        )
        self.assertRegex(
            template,
            re.compile(r"hintCell\.innerHTML = .*?markDirty\(\);\s+applyFilter\(\);", re.S),
        )
        self.assertRegex(
            template,
            re.compile(r"undoBtn\?\.addEventListener\('click', async \(\) => \{.*?markDirty\(\);\s+applyFilter\(\);", re.S),
        )


if __name__ == "__main__":
    unittest.main()
