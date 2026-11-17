from __future__ import annotations

import copy
import csv
import tempfile
import unittest
from pathlib import Path

from app.order_parser import parse_amazon_tsv, parse_ebay_csv
from app.overlay_renderer import build_overlay_lines
from app.settings_manager import DEFAULT_CONFIG


class OrderTotalsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_ebay_quantity_two_reaches_printed_subtotal(self):
        order = self.parse_ebay([self.item(Quantity="2", **{"Sold For": "$12.88", "Total Price": "$28.05"})])

        self.assertEqual(order["items"][0]["quantity"], 2)
        self.assertAlmostEqual(order["items"][0]["item_subtotal"], 25.76)
        self.assertAlmostEqual(order["subtotal_paid"], 25.76)
        self.assertAlmostEqual(order["total_paid"], 28.05)
        config = copy.deepcopy(DEFAULT_CONFIG)
        config["print_layout"].update({"total_display_mode": "subtotal",
            "field_order": ["qty", "label", "subtotal"], "line_groups_csv": "qty,label;subtotal"})
        lines = "\n".join(build_overlay_lines(order, [], config))
        self.assertIn("QTY 2", lines)
        self.assertIn("SUBTOTAL $25.76", lines)
        self.assertNotIn("SUBTOTAL $12.88", lines)

    def test_ebay_shipping_is_added_once_for_each_quantity(self):
        for quantity, expected in ((1, 13.28), (2, 19.58), (3, 25.88)):
            with self.subTest(quantity=quantity):
                order = self.parse_ebay([self.item(Quantity=str(quantity),
                    **{"Sold For": "$6.30", "Shipping And Handling": "$6.98", "Total Price": "$30.00"})])
                self.assertAlmostEqual(order["subtotal_paid"], expected)
                self.assertAlmostEqual(order["shipping_subtotal_paid"], 6.98)
                self.assertAlmostEqual(order["total_paid"], 30)

    def test_ebay_missing_grand_total_uses_full_quantity(self):
        order = self.parse_ebay([self.item(Quantity="2",
            **{"Sold For": "$12.88", "Shipping And Handling": "$4.50", "Total Price": ""})])

        self.assertAlmostEqual(order["items"][0]["line_total"], 30.26)
        self.assertAlmostEqual(order["items"][0]["line_subtotal"], 30.26)
        self.assertAlmostEqual(order["total_paid"], 30.26)
        self.assertAlmostEqual(order["subtotal_paid"], 30.26)

    def test_ebay_multiple_items_sum_quantities_and_shipping(self):
        order = self.parse_ebay([
            self.item(Quantity="2", **{"Sold For": "$12.88", "Shipping And Handling": "$4.50"}),
            self.item(Quantity="3", **{"Item Number": "123456789013", "Sold For": "$8.50"}),
        ])

        self.assertEqual(len(order["items"]), 2)
        self.assertAlmostEqual(order["item_subtotal_paid"], 51.26)
        self.assertAlmostEqual(order["shipping_subtotal_paid"], 4.50)
        self.assertAlmostEqual(order["subtotal_paid"], 55.76)
        self.assertAlmostEqual(order["total_paid"], 55.76)

    def test_ebay_order_summary_is_already_a_total(self):
        summary = self.item(Quantity="5", **{"Item Number": "", "Item Title": "",
            "Sold For": "$51.26", "Shipping And Handling": "$4.50", "Total Price": "$59.66"})
        details = [
            self.item(Quantity="2", **{"Sold For": "$12.88"}),
            self.item(Quantity="3", **{"Item Number": "123456789013", "Sold For": "$8.50"}),
        ]
        for rows in ([summary, *details], [*details, summary]):
            with self.subTest(summary_first=rows[0] is summary):
                order = self.parse_ebay(rows)
                self.assertEqual(len(order["items"]), 2)
                self.assertAlmostEqual(order["items"][0]["item_subtotal"], 25.76)
                self.assertAlmostEqual(order["items"][1]["item_subtotal"], 25.50)
                self.assertAlmostEqual(order["item_subtotal_paid"], 51.26)
                self.assertAlmostEqual(order["shipping_subtotal_paid"], 4.50)
                self.assertAlmostEqual(order["subtotal_paid"], 55.76)
                self.assertAlmostEqual(order["total_paid"], 59.66)

    def test_amazon_report_amounts_already_include_quantity(self):
        row = {"order-id": "111-1234567-1234567", "sku": "TEST-SKU", "quantity-purchased": "2",
            "item-price": "41.76", "item-promotion-discount": "1.76", "item-tax": "3.20",
            "shipping-price": "4.50", "ship-promotion-discount": "0.50", "shipping-tax": "0.32"}
        path = self.root / "amazon.txt"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row), delimiter="\t")
            writer.writeheader()
            writer.writerow(row)

        order = parse_amazon_tsv(path)[row["order-id"]]

        self.assertEqual(order["items"][0]["quantity"], 2)
        self.assertAlmostEqual(order["item_subtotal_paid"], 40)
        self.assertAlmostEqual(order["shipping_subtotal_paid"], 4)
        self.assertAlmostEqual(order["subtotal_paid"], 44)
        self.assertAlmostEqual(order["total_paid"], 47.52)

    def parse_ebay(self, rows):
        path = self.root / "ebay.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        records, warnings = parse_ebay_csv(path)
        self.assertEqual(warnings, {})
        return records["12-12345-12345"]

    def item(self, **values):
        return {"Order Number": "12-12345-12345", "Item Number": "123456789012",
            "Item Title": "Test item", "Quantity": "1", "Sold For": "$12.88",
            "Shipping And Handling": "$0.00", "Total Price": "", **values}


if __name__ == "__main__":
    unittest.main()
