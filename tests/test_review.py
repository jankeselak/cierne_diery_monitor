"""Run with: python3 -m unittest discover tests"""

import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import auth
import db
import reposts
import server


def listing(bazos_id, catalog_id=None, confidence=0.95, title="Čierne diery - Kamzík", **extra):
    return {
        "bazos_id": bazos_id, "title": title, "description": "Predám grafiku.",
        "price": 100.0, "price_text": "100 €", "date_posted": "1.10. 2026",
        "location": "Bratislava", "postal_code": "811 01", "url": "", "image_url": None,
        "views": 0, "catalog_id": catalog_id, "match_confidence": confidence, **extra,
    }


class ReviewTestCase(unittest.TestCase):
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
        self.conn.executemany("INSERT INTO catalog (id, title, archive_slug) VALUES (?, ?, ?)",
                              [(1, "Kamzík – Lucia Tallová", "kamzik-tallova"),
                               (2, "Kamzík — Michal Tornyai", "kamzik-tornyai")])
        self.conn.commit()

    def scrape(self, listings):
        """The parts of a scrape run that touch matches: overrides, then storage."""
        db.apply_overrides(listings, db.get_overrides(self.conn))
        for l in listings:
            db.upsert_listing(self.conn, l)
        self.conn.commit()

    def catalog_id(self, bazos_id):
        return self.conn.execute("SELECT catalog_id FROM listings WHERE bazos_id = ?",
                                 (bazos_id,)).fetchone()[0]


class OverrideTest(ReviewTestCase):
    def test_decision_survives_rescrape(self):
        self.scrape([listing("1", catalog_id=None, confidence=0)])
        db.set_override(self.conn, reposts.ad_group_id(listing("1")), "matched", 1)
        self.assertEqual(self.catalog_id("1"), 1)
        # Matcher guesses differently on the next scrape; the decision wins
        self.scrape([listing("1", catalog_id=2, confidence=0.95)])
        self.assertEqual(self.catalog_id("1"), 1)

    def test_decision_applies_to_reposts(self):
        self.scrape([listing("1", confidence=0)])
        db.set_override(self.conn, reposts.ad_group_id(listing("1")), "not_cd")
        self.scrape([listing("2", catalog_id=2, confidence=0.95)])  # same ad, new bazos_id
        self.assertIsNone(self.catalog_id("2"))


class QueueTest(ReviewTestCase):
    def queue_ids(self):
        return [r["bazos_id"] for r in db.get_review_queue(self.conn)]

    def test_queue_has_unmatched_and_low_confidence_only(self):
        self.scrape([
            listing("1", catalog_id=None, confidence=0, title="Čierne diery"),
            listing("2", catalog_id=1, confidence=0.7, title="Čierne diery Kamzík Tallová"),
            listing("3", catalog_id=1, confidence=0.95, title="Kamzík – Lucia Tallová"),
        ])
        self.assertEqual(sorted(self.queue_ids()), ["1", "2"])

    def test_queue_is_empty_after_review(self):
        self.scrape([listing("1", confidence=0, title="A"), listing("2", confidence=0, title="B")])
        for bazos_id, title in [("1", "A"), ("2", "B")]:
            db.set_override(self.conn, reposts.ad_group_id(listing(bazos_id, title=title)), "lot")
        self.assertEqual(self.queue_ids(), [])

    def test_inactive_ads_are_not_queued(self):
        self.scrape([listing("1", confidence=0)])
        self.conn.execute("UPDATE listings SET is_active = 0")
        self.assertEqual(self.queue_ids(), [])


class AdminAccessTest(ReviewTestCase):
    def setUp(self):
        super().setUp()
        self.conn.executemany(
            "INSERT INTO users (email, token, token_expires_at) VALUES (?, ?, '2099-01-01 00:00:00')",
            [("admin@example.com", "admin-token"), ("user@example.com", "user-token")])
        self.conn.commit()
        env = mock.patch.dict(os.environ, {"ADMIN_EMAILS": "Admin@example.com"})
        env.start()
        self.addCleanup(env.stop)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)

    def request(self, token=None, body=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.httpd.server_address[1]}/api/admin/review",
            data=json.dumps(body).encode() if body is not None else None,
            headers={"X-Auth-Token": token or "", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    def test_admin_email_check_is_case_insensitive(self):
        self.assertTrue(auth.is_admin({"email": "admin@example.com"}))
        self.assertFalse(auth.is_admin({"email": "user@example.com"}))
        self.assertFalse(auth.is_admin(None))

    def test_non_admins_get_403(self):
        self.assertEqual(self.request()[0], 403)
        self.assertEqual(self.request("user-token")[0], 403)
        self.assertEqual(self.request("user-token", {"ad_group_id": "x", "status": "lot"})[0], 403)

    def test_admin_can_list_and_decide(self):
        self.scrape([listing("1", confidence=0)])
        status, queue = self.request("admin-token")
        self.assertEqual((status, [q["bazos_id"] for q in queue]), (200, ["1"]))
        status, result = self.request("admin-token", {"ad_group_id": queue[0]["ad_group_id"],
                                                      "status": "matched", "catalog_id": 2})
        self.assertEqual((status, result), (200, {"ok": True}))
        self.assertEqual(self.catalog_id("1"), 2)
        self.assertEqual(self.request("admin-token")[1], [])

    def test_unknown_catalog_item_is_rejected(self):
        self.scrape([listing("1", confidence=0)])
        group = reposts.ad_group_id(listing("1"))
        _, result = self.request("admin-token", {"ad_group_id": group, "status": "matched", "catalog_id": 999})
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
