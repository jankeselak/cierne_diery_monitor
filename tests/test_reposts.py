"""Run with: python3 -m unittest discover tests"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
import notifier
import reposts


def listing(bazos_id, title="Čierne diery - Kamzík", description="Predám grafiku, nerozbalená.",
            price=100.0, postal_code="811 01", **extra):
    return {
        "bazos_id": bazos_id, "title": title, "description": description,
        "price": price, "price_text": f"{price:.0f} €", "date_posted": "1.10. 2026",
        "location": "Bratislava", "postal_code": postal_code, "url": "", "image_url": None,
        "views": 0, "catalog_id": None, **extra,
    }


class AdGroupIdTest(unittest.TestCase):
    def test_repost_with_same_text_is_same_group(self):
        self.assertEqual(reposts.ad_group_id(listing("1")), reposts.ad_group_id(listing("2")))

    def test_repost_with_changed_price_is_same_group(self):
        self.assertEqual(
            reposts.ad_group_id(listing("1", price=100.0)),
            reposts.ad_group_id(listing("2", price=80.0)),
        )

    def test_price_edited_in_description_is_same_group(self):
        self.assertEqual(
            reposts.ad_group_id(listing("1", description="Kniha - rozbalená - 480€\nKamzík - 190€")),
            reposts.ad_group_id(listing("2", description="Kniha - rozbalená - 450€\nKamzík - 170€")),
        )

    def test_case_diacritics_and_punctuation_are_ignored(self):
        self.assertEqual(
            reposts.ad_group_id(listing("1", title="Čierne diery - Kamzík")),
            reposts.ad_group_id(listing("2", title="CIERNE DIERY  Kamzik!")),
        )

    def test_same_generic_title_with_different_description_is_different_group(self):
        self.assertNotEqual(
            reposts.ad_group_id(listing("1", title="Čierne diery", description="Predám Kamzík")),
            reposts.ad_group_id(listing("2", title="Čierne diery", description="Predám Figaro")),
        )

    def test_different_seller_location_is_different_group(self):
        self.assertNotEqual(
            reposts.ad_group_id(listing("1", postal_code="811 01")),
            reposts.ad_group_id(listing("2", postal_code="040 01")),
        )

    def test_bundle_children_get_one_group_per_item(self):
        parent = reposts.ad_group_id(listing("1"))
        child_0 = reposts.ad_group_id(listing("1_0", parent_bazos_id="1"))
        child_1 = reposts.ad_group_id(listing("1_1", parent_bazos_id="1"))
        self.assertEqual(child_0, parent + "_0")
        self.assertNotEqual(child_0, child_1)
        # The same item in a repost of the bundle lands in the same group
        self.assertEqual(child_0, reposts.ad_group_id(listing("2_0", parent_bazos_id="2")))


class DatabaseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        patcher = mock.patch.multiple(db, DB_DIR=Path(self.tmp.name),
                                      DB_PATH=Path(self.tmp.name) / "test.db")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)
        db.init_db()
        self.conn = db.get_connection()
        self.addCleanup(self.conn.close)
        self.conn.execute("INSERT INTO catalog (id, title, archive_slug) VALUES (1, 'Kamzík', 'kamzik')")

    def test_ads_view_keeps_latest_repost_and_first_seen(self):
        db.upsert_listing(self.conn, listing("1", price=100.0, catalog_id=1))
        self.conn.execute("UPDATE listings SET first_seen = '2026-01-01 00:00:00'")
        db.upsert_listing(self.conn, listing("2", price=80.0, catalog_id=1))
        db.upsert_listing(self.conn, listing("3", title="Iný inzerát", catalog_id=1))
        rows = self.conn.execute("SELECT * FROM ads ORDER BY bazos_id").fetchall()
        self.assertEqual([r["bazos_id"] for r in rows], ["2", "3"])
        self.assertEqual(rows[0]["price"], 80.0)
        self.assertEqual(rows[0]["repost_count"], 2)
        self.assertEqual(rows[0]["group_first_seen"], "2026-01-01 00:00:00")
        prices = [r["price"] for r in db.get_price_history(self.conn, 1)]
        self.assertEqual(sorted(prices), [80.0, 100.0])  # one per ad, not one per repost

    def test_backfill_groups_existing_rows(self):
        self.conn.execute("INSERT INTO listings (bazos_id, title, description, postal_code) "
                          "VALUES ('9', 'Čierne diery - Kamzík', 'Predám grafiku, nerozbalená.', '811 01')")
        self.conn.commit()
        db.init_db()
        group = self.conn.execute("SELECT ad_group_id FROM listings WHERE bazos_id = '9'").fetchone()[0]
        self.assertEqual(group, reposts.ad_group_id(listing("9")))

    def test_repost_does_not_send_second_alert(self):
        self.conn.execute("INSERT INTO users (id, email) VALUES (1, 'a@example.com')")
        self.conn.execute("INSERT INTO watchlist (user_id, catalog_id) VALUES (1, 1)")
        first, repost = listing("1", catalog_id=1), listing("2", price=80.0, catalog_id=1)
        with mock.patch.object(notifier, "RESEND_API_KEY", "test"), \
             mock.patch.object(notifier, "send_watchdog_alert", return_value=True) as send:
            db.upsert_listing(self.conn, first)
            notifier.send_watchdog_alerts(self.conn, [first])
            db.upsert_listing(self.conn, repost)
            notifier.send_watchdog_alerts(self.conn, [repost])
        self.assertEqual(send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
