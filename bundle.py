"""Bundle listing detection, parsing, and expansion.

Detects bazos listings that sell multiple prints in one ad,
parses individual items from descriptions, and creates
virtual child listings for each identified item.
"""

import re
import matcher
import reposts


# Title patterns that suggest multiple items
_PLURAL_PATTERNS = re.compile(
    r'\bgrafiky\b|\bgrafík\b|\brisografie\b|\brisografií\b'
    r'|\brozpredat\b|\bzbierku\b|\bspolu\b',
    re.IGNORECASE,
)

# Line patterns for item entries
_NUMBERED_LINE = re.compile(r'^\s*\d+[.)]\s*(.+)', re.MULTILINE)
_BULLET_LINE = re.compile(r'^\s*[•✅]\s*(.+)', re.MULTILINE)

# Price extraction from a line — matches "60€", "180 €", "- 280 €", "400EUR"
_PRICE_INLINE = re.compile(r'[-–—]?\s*(\d[\d\s]*)\s*(?:€|eur)(?:\b|$)', re.IGNORECASE)
_PRICE_PER_PIECE = re.compile(r'(\d[\d\s]*)\s*(?:€|eur)\s*/\s*ks', re.IGNORECASE)

# Filler lines to skip
_FILLER_PATTERNS = re.compile(
    r'^\s*$|osobn[yý]\s*odber|info\s+v\s+smsk|prosím\s+nevola'
    r'|cena\s+je\s+za\s+vsetky|len\s+osobn|poštovn|predám\s+nasledov'
    r'|predám\s+nerozbal|rozhodol\s+som|ponúkam\s+na|predám\s+nasleduj'
    r'|risografie\s*cierne|grafiky\s*cierne|cierne\s*diery'
    r'|vsetky\s+su\s+v\s+povodnom|^\s*\.+\s*$'
    r'|ponúkam\s+grafik|ponukam\s+grafik',
    re.IGNORECASE,
)

# "PREDANE" / sold markers
_SOLD_PATTERN = re.compile(r'predané|predane|sold', re.IGNORECASE)


def is_bundle(title, description):
    """Detect whether a listing is selling multiple items."""
    title_lower = title.lower()
    desc_lower = description.lower()

    # Must have plural/bundle hint in title
    if not _PLURAL_PATTERNS.search(title_lower):
        return False

    # Count item-like lines in description
    item_lines = _extract_candidate_lines(description)
    return len(item_lines) >= 2


def bundle_items(listing):
    """Return parsed items if the listing sells multiple prints, else []."""
    if listing.get("is_buying"):
        return []
    description = listing.get("description") or ""
    if not is_bundle(listing["title"], description):
        return []
    items = parse_items(description)
    return items if len(items) >= 2 else []


def parse_items(description):
    """Parse individual items from a bundle description.

    Returns list of dicts: {"name": str, "artist": str|None, "price": float|None}
    """
    # Check for per-piece pricing first
    per_piece_price = None
    pp_match = _PRICE_PER_PIECE.search(description)
    if pp_match:
        per_piece_price = _parse_price_str(pp_match.group(1))

    lines = _extract_candidate_lines(description)
    items = []

    for line in lines:
        parsed = _parse_item_line(line)
        if parsed:
            # Apply per-piece price if item has no own price
            if parsed["price"] is None and per_piece_price is not None:
                parsed["price"] = per_piece_price
            items.append(parsed)

    return items


def make_child(parent_listing, parsed_item, index, catalog_items):
    """Create a virtual child listing from a parsed bundle item.

    Copies metadata from parent, sets individual catalog_id and price.
    """
    child = {
        "bazos_id": f"{parent_listing['bazos_id']}_{index}",
        "title": parent_listing["title"],
        "description": parent_listing["description"],
        "preview": reposts.preview(parent_listing),
        "price": parsed_item["price"],
        "price_text": f"{parsed_item['price']:.0f} €" if parsed_item["price"] else "Dohodou",
        "date_posted": parent_listing.get("date_posted"),
        "location": parent_listing.get("location", ""),
        "postal_code": parent_listing.get("postal_code", ""),
        "url": parent_listing.get("url", ""),
        "image_url": parent_listing.get("image_url"),
        "views": parent_listing.get("views", 0),
        "is_buying": 0,
        "is_trading": parent_listing.get("is_trading", 0),
        "is_bundle": 0,
        "parent_bazos_id": parent_listing["bazos_id"],
        "catalog_id": None,
    }

    # Match this item to catalog
    name = parsed_item["name"]
    artist_ctx = parsed_item.get("artist") or ""
    context = f"{name} {artist_ctx}".strip()
    cat_id, confidence = matcher.match_listing_to_catalog(name, context, catalog_items)
    child["catalog_id"] = cat_id

    return child


def _extract_candidate_lines(description):
    """Extract lines that look like item entries."""
    # Try structured formats first
    numbered = _NUMBERED_LINE.findall(description)
    if len(numbered) >= 2:
        return [l.strip() for l in numbered if not _is_filler(l) and not _SOLD_PATTERN.search(l)]

    bullets = _BULLET_LINE.findall(description)
    if len(bullets) >= 2:
        return [l.strip() for l in bullets if not _is_filler(l) and not _SOLD_PATTERN.search(l)]

    # Fall back to line-by-line analysis
    candidates = []
    for line in description.split('\n'):
        line = line.strip()
        if not line or len(line) > 120:
            continue
        if _is_filler(line):
            continue
        if _SOLD_PATTERN.search(line):
            continue
        if len(line) < 5:
            continue
        # Skip section headers ("Čierne diery:", "Iné grafiky:", etc.)
        if re.match(r'^[\w\s]+:\s*$', line):
            continue
        # Skip per-piece price lines
        if _PRICE_PER_PIECE.search(line):
            continue
        # Lines with separator are strong candidates
        if re.search(r'[–—/\-]', line):
            candidates.append(line)
        # Short standalone lines (< 50 chars) that look like item names
        elif len(line) < 50 and not re.search(r'^\d+\s*€|cena|predám|dohodou', line, re.IGNORECASE):
            candidates.append(line)

    return candidates


def _parse_item_line(line):
    """Parse a single item line into name, artist, price."""
    # Strip checkmarks, bullets, numbers, emojis
    line = re.sub(r'^[\d.)•✅\s]+', '', line).strip()
    line = re.sub(r'[✅✓]', '', line).strip()
    if not line or len(line) < 3:
        return None

    # Skip per-piece price lines like "75 €/ks"
    if _PRICE_PER_PIECE.search(line) and len(line) < 15:
        return None

    # Extract price (at end of line or after dash)
    price = None
    price_match = _PRICE_INLINE.search(line)
    if price_match:
        price = _parse_price_str(price_match.group(1))
        # Remove price from line for name/artist parsing
        line = line[:price_match.start()].strip().rstrip('-–— ')

    # Strip parenthetical info like "(Ondrej Gavalda, 2018, náklad 55 ks)"
    artist_from_parens = None
    paren_match = re.search(r'\(([^)]+)\)', line)
    if paren_match:
        paren_content = paren_match.group(1)
        # First part before comma is usually the artist
        parts = paren_content.split(',')
        artist_from_parens = parts[0].strip()
        line = line[:paren_match.start()].strip().rstrip(',- ')

    # Split on separator: "Building – Artist" or "Building / Artist" or "Building - Artist"
    name, artist = None, None
    for sep in [' – ', ' — ', '/ ', ' - ']:
        if sep in line:
            parts = line.split(sep, 1)
            name = parts[0].strip()
            artist = parts[1].strip() if len(parts) > 1 else None
            break

    if name is None:
        name = line.strip()

    # Use parenthetical artist if we didn't find one from separator
    if not artist and artist_from_parens:
        artist = artist_from_parens

    # Clean artist: strip trailing year, checkmarks, whitespace
    if artist:
        artist = re.sub(r'[✅✓]', '', artist).strip()
        artist = re.sub(r'\s+\d{4}\s*$', '', artist).strip()

    if not name or len(name) < 2:
        return None

    return {"name": name, "artist": artist, "price": price}


def _parse_price_str(s):
    """Parse a price string like '400', '1 250' into float."""
    cleaned = re.sub(r'\s+', '', s.strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_filler(line):
    """Check if a line is filler/metadata, not an item."""
    return bool(_FILLER_PATTERNS.search(line))
