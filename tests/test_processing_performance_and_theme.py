from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jinja2 import Environment, FileSystemLoader

from app import label_matcher
from app.batch_manager import BatchManager
from app.label_text_extractor import extract_label_signals
from test_label_inputs import settings_for
from test_walmart_and_duplicates import shipping_pdf, walmart_report


class ProcessingPerformanceTests(unittest.TestCase):
    def test_label_text_is_normalized_once_for_a_large_report(self):
        orders = {f"order-{i}": {"platform": "ebay", "order_id": f"order-{i}",
                  "ship_name": f"Customer Number {i}", "ship_postal": "90210"} for i in range(250)}
        orders["target"] = {"platform": "ebay", "order_id": "target", "ship_name": "Jane Sample", "ship_postal": "75001"}
        text = "SHIP TO JANE SAMPLE 10 EXAMPLE AVE TEST CITY TX 75001 USPS GROUND ADVANTAGE " * 4
        signals = {"text": text, "recipient_name": "Jane Sample", "ship_postal": "75001", "platform_hint": "ebay"}
        with patch("app.label_matcher._norm_text", wraps=label_matcher._norm_text) as normalize:
            result = label_matcher.match_label(Path("label.pdf"), orders, "ebay", signals=signals)
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["order"]["order_id"], "target")
        self.assertEqual(sum(call.args == (text,) for call in normalize.call_args_list), 1)

    def test_duplicate_confirmation_reuses_signals_for_unchanged_upload(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = settings_for(Path(temporary))
            manager = BatchManager(settings)
            walmart_report(settings.incoming_batch_folder / "PO_Data.xlsx")
            labels = settings.incoming_batch_folder / "labels.pdf"
            shipping_pdf(labels, names=("Jane Sample", "Jane Sample"))
            with patch("app.batch_manager.extract_label_signals", wraps=extract_label_signals) as extract:
                self.assertTrue(manager.process_batch()["needs_duplicate_confirmation"])
                first_count = extract.call_count
                self.assertTrue(manager.process_batch()["needs_duplicate_confirmation"])
                self.assertEqual(extract.call_count, first_count)
                shipping_pdf(labels, names=("Jane Sample", "Jane Sample"), tracking="9400123456781234567891")
                self.assertTrue(manager.process_batch()["needs_duplicate_confirmation"])
                self.assertGreater(extract.call_count, first_count)


class InitialThemeTests(unittest.TestCase):
    def test_saved_theme_is_available_before_the_external_stylesheet(self):
        environment = Environment(loader=FileSystemLoader(Path(__file__).resolve().parents[1] / "templates"))
        for theme, background in (("light", "#eef5f4"), ("dark", "#06101f")):
            with self.subTest(theme=theme):
                html = environment.get_template("base.html").render(ui_theme_mode=theme, tr=lambda text: text)
                self.assertIn(f'<meta name="color-scheme" content="{theme}">', html)
                self.assertLess(html.index('name="color-scheme"'), html.index('rel="stylesheet"'))
                self.assertIn(f"color-scheme: {theme}", html.split("<head>")[0])
                self.assertIn(f"background-color: {background}", html.split("<head>")[0])


if __name__ == "__main__":
    unittest.main()
