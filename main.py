#!/usr/bin/env python3
"""Čierne diery bazos.sk deal finder.

Usage:
    python main.py scrape       Scrape bazos.sk and archive, store in DB
    python main.py deals        Show current deals (best value listings)
    python main.py stats        Show price statistics per print
    python main.py list         List all active listings
"""

import sys
import db
import scraper
import matcher
import analyzer


def log(msg):
    print(msg)


def cmd_scrape():
    """Scrape both archive and bazos, match listings to catalog."""
    from datetime import datetime
    db.init_db()
    conn = db.get_connection()

    # 1. Scrape and store the catalog
    archive_items = scraper.scrape_archive()
    for item in archive_items:
        db.upsert_catalog_item(conn, item)
    conn.commit()

    # 2. Scrape bazos listings
    listings = scraper.scrape_bazos()

    # 3. Match listings to catalog
    catalog_rows = db.get_catalog(conn)
    catalog_dicts = [dict(row) for row in catalog_rows]
    listings = matcher.match_all_listings(listings, catalog_dicts)

    # 4. Store listings
    active_ids = set()
    new_count = 0
    for listing in listings:
        existing = conn.execute(
            "SELECT id FROM listings WHERE bazos_id = ?",
            (listing["bazos_id"],)
        ).fetchone()
        if not existing:
            new_count += 1
        db.upsert_listing(conn, listing)
        active_ids.add(listing["bazos_id"])

    # Mark disappeared listings as inactive
    db.mark_inactive(conn, active_ids)
    conn.commit()

    # 5. Record price snapshots
    db.record_snapshots(conn)
    conn.commit()

    matched = sum(1 for l in listings if l["catalog_id"])
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    log(f"[{now}] {len(listings)} listings ({new_count} new, {matched} matched), {len(archive_items)} catalog items")

    conn.close()


def cmd_deals():
    """Show current deals."""
    db.init_db()
    conn = db.get_connection()
    deals = analyzer.find_deals(conn)

    if not deals:
        log("No listings found. Run 'python main.py scrape' first.")
        conn.close()
        return

    # Categorize
    great = [d for d in deals if d["score"] >= 30]
    good = [d for d in deals if 15 <= d["score"] < 30]
    fair = [d for d in deals if 0 <= d["score"] < 15]
    overpriced = [d for d in deals if d["score"] < 0]

    if great:
        log("=" * 60)
        log(f"GREAT DEALS ({len(great)})")
        log("=" * 60)
        _print_deals(great)

    if good:
        log("\n" + "-" * 60)
        log(f"GOOD DEALS ({len(good)})")
        log("-" * 60)
        _print_deals(good)

    if fair:
        log(f"\n--- FAIR PRICE ({len(fair)}) ---")
        _print_deals(fair, compact=True)

    if overpriced:
        log(f"\n--- ABOVE MARKET ({len(overpriced)}) ---")
        _print_deals(overpriced[:10], compact=True)

    conn.close()


def cmd_stats():
    """Show database stats and price history."""
    db.init_db()
    conn = db.get_connection()

    summary = analyzer.get_stats_summary(conn)
    log("DATABASE SUMMARY")
    log("=" * 40)
    log(f"Catalog entries:    {summary['catalog_count']}")
    log(f"Total listings:     {summary['total_listings']}")
    log(f"Active listings:    {summary['active_listings']}")
    log(f"Matched to catalog: {summary['matched_listings']}")
    if summary['min_price'] is not None:
        log(f"Price range:        {summary['min_price']:.0f}€ – {summary['max_price']:.0f}€")
        log(f"Average price:      {summary['avg_price']:.0f}€")

    log("\n\nPRICE STATS PER PRINT (sorted by # listings)")
    log("=" * 80)
    log(f"{'Print':<40} {'#':>3} {'Min':>6} {'Med':>6} {'Max':>6} {'Avg':>6}")
    log("-" * 80)

    price_stats = db.get_price_stats(conn)
    for row in price_stats:
        # Compute median from raw data
        prices = db.get_price_history(conn, row["id"])
        price_list = [p["price"] for p in prices if p["price"]]
        from statistics import median
        med = median(price_list) if price_list else 0

        title = (row["building"] or row["title"] or "?")[:38]
        log(f"{title:<40} {row['listing_count']:>3} {row['min_price']:>5.0f}€ {med:>5.0f}€ {row['max_price']:>5.0f}€ {row['avg_price']:>5.0f}€")

    conn.close()


def cmd_list():
    """List all active listings."""
    db.init_db()
    conn = db.get_connection()
    listings = db.get_listings(conn, active_only=True)

    if not listings:
        log("No listings found. Run 'python main.py scrape' first.")
        conn.close()
        return

    log(f"{'Price':>7} {'Title':<45} {'Matched To':<30} {'Location':<15}")
    log("-" * 100)
    for l in listings:
        price_str = f"{l['price']:.0f}€" if l['price'] else "—"
        title = l["title"][:43]
        matched = (l["building"] or "—")[:28]
        loc = (l["location"] or "")[:13]
        log(f"{price_str:>7} {title:<45} {matched:<30} {loc:<15}")

    log(f"\nTotal: {len(listings)} active listings")
    conn.close()


def _print_deals(deals, compact=False):
    for d in deals:
        if compact:
            log(f"  {d['price']:>6.0f}€  {d['title'][:50]}")
        else:
            score_label = ""
            if d["score"] >= 30:
                score_label = "*** "
            elif d["score"] >= 15:
                score_label = "**  "
            elif d["score"] >= 0:
                score_label = "*   "

            log(f"\n  {score_label}{d['title']}")
            log(f"     Price: {d['price']:.0f}€  |  Median: {d['median_price']:.0f}€  |  {d['reason']}")
            if d["catalog_title"] != "—":
                log(f"     Catalog: {d['catalog_title']}")
            log(f"     {d['location']} | {d['date_posted']} | {d['views']} views")
            log(f"     {d['url']}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    commands = {
        "scrape": cmd_scrape,
        "deals": cmd_deals,
        "stats": cmd_stats,
        "list": cmd_list,
    }

    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
