import re
import unicodedata
from functools import lru_cache


@lru_cache(maxsize=None)
def normalize(text):
    """Normalize text for matching: lowercase, strip diacritics, remove noise."""
    text = text.lower().strip()
    # Remove common prefixes
    for prefix in ["čierne diery", "cierne diery", "čierne diéry", "čd", "cd"]:
        if text.startswith(prefix):
            text = text[len(prefix):].strip(" -–—:").strip()
    # Remove common suffixes
    for suffix in ["grafiky", "grafika", "risografiky", "risografika", "risografie", "risografia", "čierne diery", "cierne diery"]:
        if text.endswith(suffix):
            text = text[:-len(suffix)].strip(" -–—:").strip()
    # Strip diacritics
    text = _strip_diacritics(text)
    # Remove punctuation except spaces
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_diacritics(text):
    """Remove diacritical marks from text."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# Other publishers whose prints are traded under similar titles
OTHER_PUBLISHERS = ("bratiska", "risoto", "poustr", "madebythe", "thomas wolf")

# Names people use on bazos for a print's building (listing text -> catalog building)
ALIASES = {
    "parlament": "narodna rada",
    "roh trnava": "roh erohacko",
    "manderlak": "manderla",
}

# Words that say which kind of item a listing is about
_BOOK_HINT = re.compile(r"\bknih")
_PRINT_HINT = re.compile(r"\bgrafik|\brisograf")

# Title words that mean the ad sells a whole series or collection, not one item
_COLLECTION_HINT = re.compile(r" (seri[aeiuy]\w*|kompletn\w*|zbierk\w*) ")

# Shortest word used on its own as evidence for a building
MIN_DISTINCTIVE_LEN = 4
# A word in this many catalog buildings ("Bratislava") identifies none of them
COMMON_WORD_BUILDINGS = 4
# Shortest surname that counts as naming a different artist
MIN_SURNAME_LEN = 4
# Names that are also everyday words ("podobnej" is not Rastislav Podoba, "kusy"
# are pieces) or common first names
SURNAMES_AS_WORDS = {"podoba", "kusy", "kusa", "jana"}

# Listing words that describe the ad, not the print ("nová grafika" is not Nová doba)
_GENERIC_WORD = re.compile(r"^(grafik|risograf|predam|predaj|vymen|kupim|hladam|zhanam|nov[aeyu]$|original)")


def _text(text):
    """Normalize free listing text for phrase search (keeps all words)."""
    text = _strip_diacritics((text or "").lower())
    # Sellers paste links to their other ads ("ostatne.bazos.sk" is not Bazová)
    text = re.sub(r"https?://\S+", " ", text)
    # "ciernediery" is often glued to neighbouring words in bazos previews
    text = re.sub(r"ciernediery", " cierne diery ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = " " + re.sub(r"\s+", " ", text).strip() + " "
    for alias, building in ALIASES.items():
        text = text.replace(f" {alias} ", f" {building} ")
    return text


def _words(text):
    """Words that can identify a print: no brand name, no generic ad words."""
    return {w for w in text.replace(" cierne diery ", " ").split() if not _GENERIC_WORD.match(w)}


def _has_phrase(text, phrase):
    return bool(phrase) and f" {phrase} " in text


def _same_word(a, b):
    """Equal words, or the same word in another Slovak case form (Jelšava ~ Jelšave, Cvik ~ Cvika)."""
    if a == b:
        return True
    short, long_ = sorted((a, b), key=len)
    if len(short) == 4 and long_.startswith(short) and len(long_) - len(short) <= 2:
        return True
    if min(len(a), len(b)) < 5 or abs(len(a) - len(b)) > 2:
        return False
    stem = min(len(a), len(b)) - 1
    return a[:stem] == b[:stem]


def _mentions_word(words, target):
    return any(_same_word(w, target) for w in words)


def _surnames(item):
    """Artist surnames: "Alexandra Kusá a Michal Tornyai" -> kusa, tornyai.

    First names are left out: "Michal" would also match "Michaela". Books have
    no reliable artist in the catalog (the title split yields subtitle words).
    """
    if item.get("category") == "Kniha":
        return []
    names = []
    for person in re.split(r"\s+a\s+|&|,", item.get("artist") or ""):
        tokens = normalize(person).split()
        names += tokens[1:] if len(tokens) > 1 else tokens
    return [n for n in names if len(n) >= 3 and n not in SURNAMES_AS_WORDS]


def _artist_mentioned(item, words):
    return any(_mentions_word(words, n) for n in _surnames(item))


def _building(item):
    return normalize(item["building"] or item["title"])


def _distinct(items):
    """Collapse catalog duplicates (same building and artist listed twice)."""
    seen = {}
    for item in sorted(items, key=lambda i: i["id"]):
        seen.setdefault((_building(item), normalize(item.get("artist") or "")), item)
    return list(seen.values())


def _most_specific(items):
    """Keep items whose building phrase isn't contained in another matched building."""
    buildings = {_building(i) for i in items}
    return [i for i in items
            if not any(b != _building(i) and f" {_building(i)} " in f" {b} " for b in buildings)]


def _pick(items, text):
    """Return the single item, breaking ties by full name or book/print hints; else None."""
    items = _distinct(items)
    if len(items) > 1:
        # "Veslársky klub" fits two prints; "Nemecký veslársky klub" in the text decides
        named = [i for i in items if _has_phrase(text, _building(i))]
        if len(named) == 1:
            return named[0]
        if _BOOK_HINT.search(text):
            items = [i for i in items if i.get("category") == "Kniha"] or items
        elif _PRINT_HINT.search(text):
            items = [i for i in items if i.get("category") != "Kniha"] or items
    return items[0] if len(items) == 1 else None


_owners_cache = {}


def _word_owners(catalog_items):
    """Map each building word to the set of buildings that use it (cached per catalog)."""
    key = tuple((i["id"], i["building"], i["title"]) for i in catalog_items)
    if key not in _owners_cache:
        _owners_cache.clear()
        _owners_cache[key] = _build_word_owners(catalog_items)
    return _owners_cache[key]


def _build_word_owners(catalog_items):
    owners = {}
    for item in catalog_items:
        for w in set(_building(item).split()):
            if len(w) >= MIN_DISTINCTIVE_LEN:
                owners.setdefault(w, set()).add(_building(item))
    return owners


def match_listing_to_catalog(listing_title, listing_description, catalog_items):
    """Try to match a bazos listing to a catalog item.

    Returns (catalog_id, confidence) or (None, 0). Prefers no match over a
    guess: when several prints fit and nothing tells them apart, the listing
    names several different prints, or it names a different artist.
    """
    title = _text(listing_title)
    desc = _text(listing_description)
    both = title + desc
    title_words, desc_words, all_words = _words(title), _words(desc), _words(both)

    if any(p in title for p in OTHER_PUBLISHERS) and "cierne diery" not in title:
        return None, 0
    if _COLLECTION_HINT.search(title):
        return None, 0

    owners = _word_owners(catalog_items)

    def distinctive_words(item):
        return [w for w in _building(item).split()
                if len(owners.get(w, ())) in range(1, COMMON_WORD_BUILDINGS)]

    def building_in(text, item, min_words=1):
        b = _building(item)
        return len(b.split()) >= min_words and _has_phrase(text, b)

    def building_word_in(words, item):
        return any(_mentions_word(words, w) for w in distinctive_words(item))

    def all_building_words_in(words, item):
        dw = distinctive_words(item)
        return bool(dw) and all(_mentions_word(words, w) for w in dw)

    with_artist = [i for i in catalog_items if _artist_mentioned(i, all_words)]
    named_surnames = [n for i in with_artist for n in _surnames(i)
                      if len(n) >= MIN_SURNAME_LEN and _mentions_word(all_words, n)]

    def by_other_artist(item):
        """The listing names an artist, but not this print's artist."""
        own = _surnames(item)
        return bool(own) and bool(named_surnames) and not _artist_mentioned(item, all_words)

    def other_print_in_desc(item):
        """The description also names a different print (a lot or trade list)."""
        own_words = _building(item).split()
        for other in catalog_items:
            b = _building(other)
            if b == _building(item) or f" {b} " in f" {_building(item)} ":
                continue
            if building_in(desc, other, min_words=2):
                return True
            if other in with_artist and any(
                    _mentions_word(desc_words, w) and not _mentions_word(own_words, w)
                    for w in distinctive_words(other)):
                return True
        return False

    # 1. Building and artist both named: strongest evidence. Title first, so a
    #    trade list in the description doesn't override the item in the title.
    # A building contained in a longer one named in the title is not what's
    # sold ("Námestie slobody" in "Bratislava cez Námestie slobody")
    title_buildings = _most_specific([i for i in catalog_items if building_in(title, i)])
    def inside_longer_title_building(item):
        return any(_building(item) != _building(t) and f" {_building(item)} " in f" {_building(t)} "
                   for t in title_buildings)

    for candidates in (
        [i for i in with_artist if building_in(title, i)],
        [i for i in with_artist if building_word_in(title_words, i)],
    ):
        candidates = _most_specific(_distinct(
            [i for i in candidates if not inside_longer_title_building(i)]))
        if candidates:
            item = _pick(candidates, both)
            return (item["id"], 0.95) if item else (None, 0)
    candidates = _most_specific(_distinct(
        [i for i in with_artist
         if building_in(desc, i, min_words=2) or all_building_words_in(desc_words, i)]))
    if candidates:
        item = _pick(candidates, both)
        if item and not other_print_in_desc(item):
            return item["id"], 0.95
        return None, 0

    def longer_name_fits(item, words):
        """A longer building containing this name fits the listing better ("Synagóga Košice")."""
        b = _building(item)
        return any(
            _mentions_word(words, w)
            for other in catalog_items
            if _building(other) != b and f" {b} " in f" {_building(other)} "
            for w in _building(other).split() if len(w) >= MIN_DISTINCTIVE_LEN and w not in b.split()
        )

    # 2. Exact building name (title, then multi-word names in the description)
    for candidates, from_desc in (
        ([i for i in catalog_items if building_in(title, i)], False),
        ([i for i in catalog_items if building_in(desc, i, min_words=2)], True),
    ):
        candidates = _most_specific(candidates)
        if candidates:
            item = _pick(candidates, both)
            if (not item or by_other_artist(item)
                    or longer_name_fits(item, desc_words if from_desc else title_words)
                    or (from_desc and other_print_in_desc(item))):
                return None, 0
            return item["id"], 0.85

    # 3. A distinctive word from the title that belongs to exactly one building
    #    ("Rendez", "Jelšava", "Devín")
    matched_buildings = set()
    for w, buildings in owners.items():
        if len(buildings) == 1 and _mentions_word(title_words, w):
            matched_buildings |= buildings
    if len(matched_buildings) == 1:
        b = matched_buildings.pop()
        item = _pick([i for i in catalog_items if _building(i) == b], both)
        if item and not by_other_artist(item):
            return item["id"], 0.7

    return None, 0


def match_all_listings(listings, catalog_items, on_progress=None):
    """Match all listings to catalog items. Returns updated listings with catalog_id set."""
    matched = 0
    for listing in listings:
        cat_id, confidence = match_listing_to_catalog(
            listing["title"], listing.get("description", ""), catalog_items
        )
        listing["match_confidence"] = confidence
        if cat_id:
            listing["catalog_id"] = cat_id
            matched += 1

    if on_progress:
        on_progress(f"Matched {matched}/{len(listings)} listings to catalog ({matched/len(listings)*100:.0f}%)")
    return listings
