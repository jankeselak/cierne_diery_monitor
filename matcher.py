import re
import unicodedata


def normalize(text):
    """Normalize text for matching: lowercase, strip diacritics, remove noise."""
    text = text.lower().strip()
    # Remove common prefixes
    for prefix in ["čierne diery", "cierne diery", "čierne diéry", "cd"]:
        if text.startswith(prefix):
            text = text[len(prefix):].strip(" -–—:").strip()
    # Remove common suffixes
    for suffix in ["grafika", "risografika", "risografia", "čierne diery", "cierne diery"]:
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


def _words(text):
    """Split into word set."""
    return set(normalize(text).split())


def match_listing_to_catalog(listing_title, listing_description, catalog_items):
    """Try to match a bazos listing to a catalog item.

    Returns (catalog_id, confidence) or (None, 0).
    Confidence: 1.0 = exact, 0.7+ = good match, 0.5+ = partial.
    """
    listing_norm = normalize(listing_title)
    listing_words = _words(listing_title)
    desc_norm = normalize(listing_description)
    desc_words = _words(listing_description)
    all_words = listing_words | desc_words

    best_match = None
    best_score = 0

    for item in catalog_items:
        building_norm = normalize(item["building"] or item["title"])
        building_words = set(building_norm.split())

        if not building_words:
            continue

        # Strategy 1: Exact building name in normalized title
        if building_norm and building_norm in listing_norm:
            score = 0.95
            if best_score < score:
                best_match = item["id"]
                best_score = score
            continue

        # Strategy 2: Exact building name in normalized description
        if building_norm and building_norm in desc_norm:
            score = 0.85
            if best_score < score:
                best_match = item["id"]
                best_score = score
            continue

        # Strategy 3: Word overlap with building name
        if building_words:
            overlap = building_words & all_words
            jaccard = len(overlap) / len(building_words | all_words)
            # Require most building words to be present
            coverage = len(overlap) / len(building_words)

            if coverage >= 0.8 and len(overlap) >= 2:
                score = 0.6 + (jaccard * 0.3)
                if best_score < score:
                    best_match = item["id"]
                    best_score = score

        # Strategy 4: Check artist name in listing
        if item.get("artist"):
            artist_norm = normalize(item["artist"])
            artist_words = set(artist_norm.split())
            if artist_words and artist_norm in listing_norm or artist_norm in desc_norm:
                # Artist match alone is weaker, but combined with partial building match
                artist_score = 0.4
                if building_words:
                    overlap = building_words & all_words
                    if overlap:
                        artist_score = 0.5 + (len(overlap) / len(building_words)) * 0.3
                if best_score < artist_score:
                    best_match = item["id"]
                    best_score = artist_score

    # Only return matches above threshold
    if best_score >= 0.5:
        return best_match, best_score
    return None, 0


def match_all_listings(listings, catalog_items, on_progress=None):
    """Match all listings to catalog items. Returns updated listings with catalog_id set."""
    matched = 0
    for listing in listings:
        cat_id, confidence = match_listing_to_catalog(
            listing["title"], listing.get("description", ""), catalog_items
        )
        if cat_id:
            listing["catalog_id"] = cat_id
            matched += 1

    if on_progress:
        on_progress(f"Matched {matched}/{len(listings)} listings to catalog ({matched/len(listings)*100:.0f}%)")
    return listings
