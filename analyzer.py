import re
from statistics import median

# Listings with these patterns in title/description are not real sale offers
_NOT_SELLING = re.compile(
    r"kúpim|kupim|hľadám|hladam|hľadám|zháňam|zhanam|výmenu|vymenu|na výmenu|"
    r"hľadám na výmenu|trade|wtb|want to buy",
    re.IGNORECASE,
)

# Prices this low are almost certainly placeholders
MIN_REAL_PRICE = 5


def _is_real_sale(listing):
    """Filter out 'want to buy', trade, and placeholder listings."""
    title = listing["title"] or ""
    desc = listing["description"] or ""
    if _NOT_SELLING.search(title) or _NOT_SELLING.search(desc):
        return False
    if listing["price"] is not None and listing["price"] < MIN_REAL_PRICE:
        return False
    return True


def find_deals(conn):
    """Analyze active listings and score them as deals.

    Returns list of dicts sorted by deal score (best deals first):
    {listing, catalog_title, score, median_price, reason}
    """
    # Get all active listings with prices
    active = conn.execute("""
        SELECT l.*, c.title as catalog_title, c.building, c.artist, c.category
        FROM listings l
        LEFT JOIN catalog c ON l.catalog_id = c.id
        WHERE l.is_active = 1 AND l.price IS NOT NULL
        ORDER BY l.price ASC
    """).fetchall()

    if not active:
        return []

    # Filter to real sale offers only
    active = [l for l in active if _is_real_sale(l)]

    # Get price history per catalog item (all listings, not just active)
    # Also filter out non-sale prices from history
    catalog_prices = {}
    rows = conn.execute("""
        SELECT l.catalog_id, l.price, l.title, l.description
        FROM listings l
        WHERE l.catalog_id IS NOT NULL AND l.price IS NOT NULL
    """).fetchall()
    for row in rows:
        if _is_real_sale(row):
            catalog_prices.setdefault(row["catalog_id"], []).append(row["price"])

    # Overall market stats for unmatched listings
    all_prices = [r["price"] for r in active if r["price"]]
    overall_median = median(all_prices) if all_prices else 50

    deals = []
    for listing in active:
        price = listing["price"]
        cat_id = listing["catalog_id"]
        reason_parts = []

        if cat_id and cat_id in catalog_prices:
            prices = catalog_prices[cat_id]
            if len(prices) >= 2:
                med = median(prices)
                if med > 0:
                    pct_below = (med - price) / med * 100
                    score = pct_below
                    if pct_below > 0:
                        reason_parts.append(f"{pct_below:.0f}% below median ({med:.0f}€, n={len(prices)})")
                    else:
                        reason_parts.append(f"{abs(pct_below):.0f}% above median ({med:.0f}€, n={len(prices)})")
                else:
                    score = 0
                    med = 0
            else:
                # Only 1 listing for this print - compare to overall median
                med = overall_median
                score = (overall_median - price) / overall_median * 100 * 0.5  # Reduced confidence
                reason_parts.append(f"Only {len(prices)} listing(s) for this print")
        else:
            # Unmatched listing - compare to overall median
            med = overall_median
            score = (overall_median - price) / overall_median * 100 * 0.3  # Low confidence
            reason_parts.append("Not matched to catalog")

        # Bonus for high view counts (demand signal)
        if listing["views"] and listing["views"] > 80:
            reason_parts.append(f"High interest ({listing['views']} views)")
            score += 5

        deals.append({
            "bazos_id": listing["bazos_id"],
            "title": listing["title"],
            "catalog_title": listing["catalog_title"] or "—",
            "building": listing["building"] or "",
            "artist": listing["artist"] or "",
            "price": price,
            "median_price": med,
            "score": score,
            "reason": "; ".join(reason_parts),
            "location": listing["location"],
            "url": listing["url"],
            "views": listing["views"],
            "date_posted": listing["date_posted"],
            "category": listing["category"] or "?",
        })

    # Sort by score descending (best deals first)
    deals.sort(key=lambda d: d["score"], reverse=True)
    return deals


def get_stats_summary(conn):
    """Get summary stats for the database."""
    stats = {}

    row = conn.execute("SELECT COUNT(*) as n FROM catalog").fetchone()
    stats["catalog_count"] = row["n"]

    row = conn.execute("SELECT COUNT(*) as n FROM listings").fetchone()
    stats["total_listings"] = row["n"]

    row = conn.execute("SELECT COUNT(*) as n FROM listings WHERE is_active = 1").fetchone()
    stats["active_listings"] = row["n"]

    row = conn.execute("SELECT COUNT(*) as n FROM listings WHERE catalog_id IS NOT NULL").fetchone()
    stats["matched_listings"] = row["n"]

    row = conn.execute("""
        SELECT MIN(price) as min_p, MAX(price) as max_p, AVG(price) as avg_p
        FROM listings WHERE price IS NOT NULL AND is_active = 1
    """).fetchone()
    stats["min_price"] = row["min_p"]
    stats["max_price"] = row["max_p"]
    stats["avg_price"] = row["avg_p"]

    row = conn.execute("""
        SELECT COUNT(*) as n FROM listings
        WHERE first_seen >= datetime('now', '-7 days')
    """).fetchone()
    stats["new_this_week"] = row["n"]

    return stats
