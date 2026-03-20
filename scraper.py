import re
import time
import requests
from bs4 import BeautifulSoup

BUY_INTENT_KEYWORDS = [
    "kúpim", "kupim", "hľadám", "hladam", "zoženiem", "zozeniam",
    "dopyt", "zháňam", "zhanam", "kúpime", "kupime", "hľadáme", "hladame",
]

TRADE_INTENT_KEYWORDS = [
    "výmena", "vymena", "vymením", "vymenim", "vymeníme", "vymenime",
    "menujem", "menujeme", "vymieňam", "vymienam",
]

def is_buy_intent(title):
    """Check if a listing title indicates buying intent."""
    t = title.lower()
    return any(kw in t for kw in BUY_INTENT_KEYWORDS)

def is_trade_intent(title):
    """Check if a listing title indicates trade intent."""
    t = title.lower()
    return any(kw in t for kw in TRADE_INTENT_KEYWORDS)


BASE_URL = "https://ostatne.bazos.sk"
ARCHIVE_URL = "https://eshop.ciernediery.sk/archive/"
SEARCH_PATH = "/inzeraty/cierne-diery/"
ITEMS_PER_PAGE = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "sk-SK,sk;q=0.9",
}

# Delay between requests to be polite
REQUEST_DELAY = 1.5


def fetch_page(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.encoding = "utf-8"
    resp.raise_for_status()
    return resp.text


def parse_bazos_page(html):
    """Parse a single bazos.sk listing page. Returns list of listing dicts."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    for item in soup.select("div.inzeraty.inzeratyflex"):
        listing = {}

        # Title and URL
        title_link = item.select_one("h2.nadpis a")
        if not title_link:
            continue
        listing["title"] = title_link.get_text(strip=True)
        listing["url"] = BASE_URL + title_link["href"]

        # Extract bazos_id from URL: /inzerat/189788133/...
        id_match = re.search(r"/inzerat/(\d+)/", title_link["href"])
        listing["bazos_id"] = id_match.group(1) if id_match else None
        if not listing["bazos_id"]:
            continue

        # Date
        date_span = item.select_one("span.velikost10")
        if date_span:
            date_match = re.search(r"\[(\d+\.\d+\.\s*\d{4})\]", date_span.get_text())
            if date_match:
                listing["date_posted"] = date_match.group(1).strip()

        # Description
        desc_div = item.select_one("div.popis")
        listing["description"] = desc_div.get_text(strip=True) if desc_div else ""

        # Price
        price_el = item.select_one("div.inzeratycena span[translate='no']")
        if price_el:
            price_text = price_el.get_text(strip=True)
            listing["price_text"] = price_text
            # Parse numeric price: "79 €" or "1 250 €" or "Dohodou"
            price_clean = price_text.replace("€", "").replace("\xa0", "").strip()
            price_clean = re.sub(r"\s+", "", price_clean)
            try:
                listing["price"] = float(price_clean)
            except ValueError:
                listing["price"] = None
        else:
            listing["price_text"] = "Dohodou"
            listing["price"] = None

        # Location
        loc_div = item.select_one("div.inzeratylok")
        if loc_div:
            loc_text = loc_div.get_text(separator="|", strip=True)
            parts = loc_text.split("|")
            listing["location"] = parts[0].strip() if parts else ""
            listing["postal_code"] = parts[1].strip() if len(parts) > 1 else ""
        else:
            listing["location"] = ""
            listing["postal_code"] = ""

        # Views
        views_div = item.select_one("div.inzeratyview")
        if views_div:
            views_match = re.search(r"(\d+)", views_div.get_text())
            listing["views"] = int(views_match.group(1)) if views_match else 0
        else:
            listing["views"] = 0

        # Image
        img = item.select_one("img.obrazek")
        listing["image_url"] = img["src"] if img else None

        listing["catalog_id"] = None  # Will be matched later
        listing["is_buying"] = 1 if is_buy_intent(listing["title"]) else 0
        listing["is_trading"] = 1 if is_trade_intent(listing["title"]) else 0
        listings.append(listing)

    return listings


def get_total_count(html):
    """Extract total listing count from page."""
    match = re.search(r"z\s+(\d+)", html)
    return int(match.group(1)) if match else 0


def scrape_bazos(max_pages=None, on_progress=None):
    """Scrape all bazos.sk listings. Returns list of all listings."""
    all_listings = []
    first_html = fetch_page(f"{BASE_URL}{SEARCH_PATH}")
    total = get_total_count(first_html)
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

    if max_pages:
        total_pages = min(total_pages, max_pages)

    if on_progress:
        on_progress(f"Found {total} listings across {total_pages} pages")

    # Parse first page
    page_listings = parse_bazos_page(first_html)
    all_listings.extend(page_listings)
    if on_progress:
        on_progress(f"  Page 1/{total_pages}: {len(page_listings)} listings")

    # Fetch remaining pages (pagination uses GET params with crp=offset)
    for page_num in range(2, total_pages + 1):
        offset = (page_num - 1) * ITEMS_PER_PAGE
        time.sleep(REQUEST_DELAY)
        url = f"{BASE_URL}/?hledat=cierne+diery&rubriky=ostatne&crp={offset}"
        html = fetch_page(url)
        page_listings = parse_bazos_page(html)
        all_listings.extend(page_listings)
        if on_progress:
            on_progress(f"  Page {page_num}/{total_pages}: {len(page_listings)} listings")

    return all_listings


def parse_archive_page(html):
    """Parse the eshop archive page. Returns list of catalog item dicts."""
    items = []

    # Pattern: <li class="..."> <a href="url"> <img src="img"> ... <h2>Title</h2> <p>Location</p>
    pattern = re.compile(
        r'<li\s+class="([^"]*?)">\s*'
        r'(?:<[^>]*>)*?'
        r'<a\s+href="([^"]*?)"'
        r'.*?'
        r'<img[^>]*?src="([^"]*?)"'
        r'.*?'
        r'<h2>(.*?)</h2>\s*'
        r'<p>(.*?)(?:<span>|</p>)',
        re.DOTALL
    )

    for match in pattern.finditer(html):
        li_class = match.group(1).strip()
        url = match.group(2).strip()
        image_url = match.group(3).strip()
        title = match.group(4).strip()
        location = match.group(5).strip()

        # Skip non-portfolio items (menu items, etc.)
        if "menu-item" in li_class:
            continue

        # Extract slug from URL (items with url="#" have no detail page)
        slug_match = re.search(r"/archive/([^/]+)/?$", url)
        slug = slug_match.group(1) if slug_match else None
        if not slug:
            # Use a slug derived from the title for items without a detail page
            slug = re.sub(r"[^\w\s-]", "", title.lower())
            slug = re.sub(r"[\s]+", "-", slug.strip())[:80]
            if not slug:
                continue

        # Parse "Building – Artist" from title
        # The dash used is "–" (en dash), but also handle "-"
        building, artist = _split_title_artist(title)

        # Guess category from li_class or location
        category = _guess_category(li_class, location, title)

        items.append({
            "title": title,
            "building": building,
            "artist": artist,
            "location": location if category != location else "",
            "archive_url": url if url.startswith("http") else f"https://eshop.ciernediery.sk{url}",
            "archive_slug": slug,
            "image_url": image_url,
            "category": category,
        })

    return items


def _split_title_artist(title):
    """Split 'Building Name – Artist Name' into (building, artist)."""
    # Try en dash first, then regular dash
    for sep in [" – ", " — ", " - "]:
        if sep in title:
            parts = title.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    return title, ""


def _guess_category(li_class, location, title):
    """Guess category from available data."""
    location_lower = location.lower()
    title_lower = title.lower()
    li_lower = li_class.lower()

    if li_lower == "kniha" or location_lower == "kniha":
        return "Kniha"
    if li_lower == "mapa" or location_lower == "mapa":
        return "Mapa"
    if li_lower == "zine" or location_lower == "zine":
        return "Zine"
    if "monotyp" in li_lower or "monotyp" in location_lower:
        return "Monotypia"
    if "sietotlac" in li_lower or "sieťotlač" in location_lower:
        return "Sieťotlač"
    if "ram" in li_lower and "ramov" not in li_lower:
        return "Rám"
    # Default: it's a grafika (risograph print)
    return "Grafika"


def scrape_archive(on_progress=None):
    """Scrape the eshop archive. Returns list of catalog items."""
    if on_progress:
        on_progress("Fetching archive from eshop.ciernediery.sk...")
    html = fetch_page(ARCHIVE_URL)
    items = parse_archive_page(html)
    if on_progress:
        on_progress(f"Found {len(items)} items in archive")
    return items
