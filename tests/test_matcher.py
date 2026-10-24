"""Run with: python3 -m unittest discover tests"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matcher

CATALOG = [
    {"id": 1, "title": "Lekáreň u Salvatora – Ondrej Jób", "building": "Lekáreň u Salvatora", "artist": "Ondrej Jób", "category": "Grafika"},
    {"id": 2, "title": "Lekáreň u Salvatora — Lucia Žatkuliaková", "building": "Lekáreň u Salvatora", "artist": "Lucia Žatkuliaková", "category": "Grafika"},
    {"id": 3, "title": "AVION", "building": "AVION", "artist": "", "category": "Kniha"},
    {"id": 4, "title": "Avion – Daniela Olejníková", "building": "Avion", "artist": "Daniela Olejníková", "category": "Grafika"},
    {"id": 5, "title": "Kaštieľ v Jelšave — Daniela Olejníková", "building": "Kaštieľ v Jelšave", "artist": "Daniela Olejníková", "category": "Grafika"},
    {"id": 6, "title": "Rám na grafiky Čiernych dier", "building": "Rám na grafiky Čiernych dier", "artist": "", "category": "Grafika"},
    {"id": 7, "title": "Kamzík – Lucia Tallová", "building": "Kamzík", "artist": "Lucia Tallová", "category": "Grafika"},
    {"id": 8, "title": "Bazová — Peter Cvik", "building": "Bazová", "artist": "Peter Cvik", "category": "Grafika"},
    {"id": 9, "title": "Pivovar Stein — Daniela Olejníková", "building": "Pivovar Stein", "artist": "Daniela Olejníková", "category": "Grafika"},
    {"id": 10, "title": "Námestie slobody — Saki Matsumoto", "building": "Námestie slobody", "artist": "Saki Matsumoto", "category": "Grafika"},
    {"id": 11, "title": "Bratislava cez Námestie slobody – Daniela Olejníková", "building": "Bratislava cez Námestie slobody", "artist": "Daniela Olejníková", "category": "Grafika"},
    {"id": 12, "title": "Kalvária Banská Štiavnica – Saki Matsumoto", "building": "Kalvária Banská Štiavnica", "artist": "Saki Matsumoto", "category": "Grafika"},
]


def match(title, description=""):
    return matcher.match_listing_to_catalog(title, description, CATALOG)[0]


class MatcherTest(unittest.TestCase):
    def test_artist_picks_between_prints_of_the_same_building(self):
        self.assertEqual(match("Čierne diery - Lekáreň u Salvatora - Lucia Žatkuliaková"), 2)
        self.assertEqual(match("Lekaren u Salvatora", "Salvator od Lucie Zatkuliakovej"), 2)

    def test_same_building_without_artist_is_left_unmatched(self):
        self.assertIsNone(match("Čierne diery - Lekáreň u Salvatora"))

    def test_print_and_book_of_the_same_name(self):
        self.assertEqual(match("Čierne diery - Avion", "Predám grafiku Avion od Daniely Olejníkovej"), 4)
        self.assertEqual(match("Kniha AVION", "Predám knihu"), 3)

    def test_inflected_name_matches(self):
        self.assertEqual(match("Čierne diery Jelšava"), 5)

    def test_generic_title_matches_nothing(self):
        self.assertIsNone(match("Grafiky Čierne diery", "Rozpredávam zbierku, cenu navrhnite"))

    def test_listing_naming_several_prints_matches_nothing(self):
        self.assertIsNone(match("Čierne diery - výmena",
                                "Vymením Kamzík — Lucia Tallová a Bazová — Peter Cvik"))

    def test_other_publisher_matches_nothing(self):
        self.assertIsNone(match("RISOTO Chata Kamzík"))

    def test_links_to_other_ads_are_ignored(self):
        # "ostatne.bazos.sk" must not look like "Bazová"
        self.assertIsNone(match("Čierne diery", "Ďalšie: https://ostatne.bazos.sk/inzerat/1/cierne-diery.php"))

    def test_different_artist_named_matches_nothing(self):
        # Cvik's Stein print isn't in this catalog; it is not Olejníková's
        self.assertIsNone(match("Čierne diery - Pivovar Stein", "Predám grafiku od Petra Cvika"))

    def test_shorter_building_inside_a_longer_title_name_is_not_picked(self):
        self.assertNotEqual(match("Bratislava cez Námestie slobody",
                                  "Hľadám Kalvária Banská Štiavnica — Saki Matsumoto"), 10)


if __name__ == "__main__":
    unittest.main()
