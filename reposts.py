"""Group bazos reposts into ads.

Sellers re-post the same ad to bump it, and each repost gets a new bazos_id.
Listings with the same normalized title, description and postal code belong
to one ad group. Prices are not part of the key (neither the price field nor
numbers in the text), so a repost with a new price stays in the same group.
"""

import hashlib
import re
import unicodedata


def _normalize(text):
    text = unicodedata.normalize("NFKD", (text or "").lower())
    text = "".join(c for c in text if not unicodedata.combining(c))
    # Digits are dropped so price edits in the text ("480€" -> "450€") keep the group
    text = re.sub(r"[^\w\s]|\d", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def preview(listing):
    """The search-page preview of the description (older rows only have the description)."""
    if listing.get("preview") is not None:
        return listing["preview"]
    return listing.get("description")


def ad_group_id(listing):
    """Return a stable group id for a listing; bundle children get one per item."""
    key = "|".join([
        _normalize(listing["title"]),
        # The preview, not the full text fetched for long ads, so old groups stay
        _normalize(preview(listing)),
        (listing.get("postal_code") or "").replace(" ", ""),
    ])
    group = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    if listing.get("parent_bazos_id"):
        # Child ids are "<parent bazos_id>_<item index>"
        group += "_" + listing["bazos_id"].rsplit("_", 1)[1]
    return group
