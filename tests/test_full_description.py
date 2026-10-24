"""Run with: python3 -m unittest discover tests"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
import reposts
import scraper

FULL = "Predám grafiky:\n\nKaštieľ v Jelšave – Olejníková – 140\nSmaltovňa – Olejníková – 80\nBenzinka"
PREVIEW = "Predám grafiky:\r\n\r\nKaštieľ v Jelšave – Olejníková – 140\r\nSmaltovňa – Olejní..."
DETAIL_HTML = """<div class="popisdetail">Predám grafiky: <br/>
<br/>
Kaštieľ v Jelšave – Olejníková – 140<br/>
Smaltovňa – Olejníková – 80<br/>
Benzinka <br/>
</div>"""


def listing(bazos_id, preview=PREVIEW, title="Čierne diery grafiky"):
    return {
        "bazos_id": bazos_id, "title": title, "description": preview, "preview": preview,
        "price": 100.0, "price_text": "100 €", "date_posted": "1.10. 2026",
        "location": "Bratislava", "postal_code": "811 01", "image_url": None,
        "url": f"https://ostatne.bazos.sk/inzerat/{bazos_id}/x.php", "views": 0,
        "catalog_id": None, "is_buying": 0, "is_trading": 0,
    }


class ParseTest(unittest.TestCase):
    def test_line_breaks_come_from_br_not_html_source(self):
        self.assertEqual(scraper.parse_ad_description(DETAIL_HTML), FULL)

    def test_missing_description_is_none(self):
        self.assertIsNone(scraper.parse_ad_description("<html><body>Inzerát bol zmazaný</body></html>"))


class FillTest(unittest.TestCase):
    def setUp(self):
        for patcher in (mock.patch.object(scraper.time, "sleep"),
                        mock.patch.object(scraper, "fetch_page", return_value=DETAIL_HTML)):
            self.addCleanup(patcher.stop)
            setattr(self, patcher.attribute, patcher.start())

    def test_cut_off_preview_is_replaced_and_kept_for_grouping(self):
        ad = listing("1")
        self.assertEqual(scraper.fill_full_descriptions([ad], {}), (1, 0))
        self.assertEqual((ad["description"], ad["preview"]), (FULL, PREVIEW))

    def test_complete_preview_is_not_fetched(self):
        ad = listing("1", preview="Predám Kamzík, 100 €")
        self.assertEqual(scraper.fill_full_descriptions([ad], {}), (0, 0))
        self.fetch_page.assert_not_called()

    def test_known_ad_and_its_reposts_are_not_fetched_again(self):
        ads = [listing("1"), listing("2")]
        self.assertEqual(scraper.fill_full_descriptions(ads, {PREVIEW: FULL}), (0, 0))
        self.fetch_page.assert_not_called()
        self.assertEqual([a["description"] for a in ads], [FULL, FULL])

    def test_failed_fetch_keeps_preview(self):
        self.fetch_page.side_effect = requests.ConnectionError
        ad = listing("1")
        self.assertEqual(scraper.fill_full_descriptions([ad], {}), (0, 1))
        self.assertEqual(ad["description"], PREVIEW)

    def test_fetches_per_run_are_capped(self):
        ads = [listing(str(i), preview=f"Ad {i} ...") for i in range(scraper.MAX_DETAIL_FETCHES + 5)]
        self.assertEqual(scraper.fill_full_descriptions(ads, {}), (scraper.MAX_DETAIL_FETCHES, 0))

    def test_intent_is_read_from_full_text(self):
        self.fetch_page.return_value = '<div class="popisdetail">Predám Kamzík.<br/>Vymením aj za Avion.</div>'
        ad = listing("1", preview="Predám Kamzík ...")
        scraper.fill_full_descriptions([ad], {})
        self.assertEqual(ad["is_trading"], 1)


class StorageTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = mock.patch.multiple(db, DB_DIR=Path(tmp.name), DB_PATH=Path(tmp.name) / "test.db")
        patcher.start()
        self.addCleanup(patcher.stop)
        db.init_db()
        self.conn = db.get_connection()
        self.addCleanup(self.conn.close)

    def test_full_text_keeps_the_group_of_the_preview_only_row(self):
        # Rows stored before this change have the preview as description and no preview column
        old = dict(listing("1"), preview=None)
        new = dict(listing("2"), description=FULL)
        self.assertEqual(reposts.ad_group_id(old), reposts.ad_group_id(new))

    def test_stored_full_text_is_found_by_preview(self):
        db.upsert_listing(self.conn, dict(listing("1"), description=FULL))
        db.upsert_listing(self.conn, listing("2", preview="Predám Kamzík"))
        self.assertEqual(db.get_full_descriptions(self.conn), {PREVIEW: FULL})


if __name__ == "__main__":
    unittest.main()
