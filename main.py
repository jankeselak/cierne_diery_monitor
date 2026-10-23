#!/usr/bin/env python3
"""Čierne diery bazos.sk deal finder.

Usage:
    python main.py scrape       Scrape bazos.sk and archive, store in DB
    python main.py deals        Show current deals (best value listings)
    python main.py stats        Show price statistics per print
    python main.py list         List all active listings
    python main.py eval         Measure matching accuracy against tests/match_labels.csv
"""

import os
import sys
import db
import scraper
import matcher
import analyzer
import notifier
import bundle
import reposts
import thumbs


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

    # 1b. Generate thumbnails for catalog images
    thumbs.ensure_thumbnails(archive_items)

    # 2. Scrape bazos listings
    listings = scraper.scrape_bazos()

    # 3. Match listings to catalog
    catalog_rows = db.get_catalog(conn)
    catalog_dicts = [dict(row) for row in catalog_rows]
    listings = matcher.match_all_listings(listings, catalog_dicts)

    # 3b. Detect and expand bundle listings
    expanded = []
    bundle_count = 0
    for listing in listings:
        items = bundle.bundle_items(listing)
        if items:
            bundle_count += 1
            # Mark parent as bundle, clear its catalog match
            listing["is_bundle"] = 1
            listing["catalog_id"] = None
            listing["price"] = None
            # Create/update child listings (upsert preserves first_seen)
            child_ids = set()
            for i, item in enumerate(items):
                child = bundle.make_child(listing, item, i, catalog_dicts)
                expanded.append(child)
                child_ids.add(child["bazos_id"])
            # Remove stale children (items removed from bundle)
            db.delete_stale_bundle_children(conn, listing["bazos_id"], child_ids)
        expanded.append(listing)
    listings = expanded

    # 4. Store listings
    active_ids = set()
    new_count = 0
    repost_count = 0
    new_listings = []
    known_groups = {row[0] for row in conn.execute("SELECT DISTINCT ad_group_id FROM listings")}
    for listing in listings:
        existing = conn.execute(
            "SELECT id FROM listings WHERE bazos_id = ?",
            (listing["bazos_id"],)
        ).fetchone()
        if not existing:
            # A repost of a known ad is not new: no "new" count, no alert
            group = reposts.ad_group_id(listing)
            if group in known_groups:
                repost_count += 1
            else:
                new_count += 1
                new_listings.append(listing)
                known_groups.add(group)
        db.upsert_listing(conn, listing)
        active_ids.add(listing["bazos_id"])

    # Mark disappeared listings as inactive
    db.mark_inactive(conn, active_ids)
    conn.commit()

    # 5. Send watchdog notifications for new listings
    notifier.send_watchdog_alerts(conn, new_listings)

    matched = sum(1 for l in listings if l.get("catalog_id"))
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    log(f"[{now}] {len(listings)} listings ({new_count} new, {repost_count} reposts, {matched} matched, {bundle_count} bundles), {len(archive_items)} catalog items")

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


LABELS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests", "match_labels.csv")
# Expected labels that mean "should not be matched to a catalog item"
NON_MATCH_LABELS = ("NONE", "AMBIGUOUS", "MISSING")


def predict_label(listing, catalog_items, slug_by_id):
    """Classify a listing the way the scrape pipeline does: LOT, NONE, or a catalog slug."""
    if bundle.bundle_items(listing):
        return "LOT"
    cat_id, _ = matcher.match_listing_to_catalog(
        listing["title"], listing.get("description", ""), catalog_items
    )
    return slug_by_id[cat_id] if cat_id else "NONE"


def cmd_eval():
    """Measure matching accuracy against the hand-labeled listings."""
    import csv
    db.init_db()
    conn = db.get_connection()
    catalog_items = [dict(row) for row in db.get_catalog(conn)]
    conn.close()
    slug_by_id = {item["id"]: item["archive_slug"] for item in catalog_items}

    with open(LABELS_PATH, newline="") as f:
        rows = list(csv.DictReader(f))

    matched = correct = expected_slug = 0
    lot_expected = lot_found = 0
    errors = []
    for row in rows:
        expected = row["expected"]
        kind = expected.split(":")[0]
        listing = {
            "title": row["title"],
            "description": row["description"],
            "is_buying": 1 if scraper.is_buy_intent(row["title"], row["description"]) else 0,
        }
        predicted = predict_label(listing, catalog_items, slug_by_id)

        if kind == "LOT":
            lot_expected += 1
            ok = predicted == "LOT"
            lot_found += ok
        elif kind in NON_MATCH_LABELS:
            ok = predicted in ("NONE", "LOT")
        else:
            expected_slug += 1
            ok = predicted in expected.split("|")

        if predicted not in ("NONE", "LOT"):
            matched += 1
            correct += predicted in expected.split("|")

        if not ok:
            errors.append((row, predicted))

    precision = correct / matched if matched else 0
    recall = correct / expected_slug if expected_slug else 0
    log(f"Labeled listings:  {len(rows)}")
    log(f"Precision:         {precision:.0%}  ({correct}/{matched} matches correct)")
    log(f"Recall:            {recall:.0%}  ({correct}/{expected_slug} matchable listings matched)")
    log(f"Lots detected:     {lot_found}/{lot_expected}")
    log(f"\nERRORS ({len(errors)})")
    log("-" * 80)
    for row, predicted in errors:
        log(f"  {row['title'][:60]}")
        log(f"     expected: {row['expected']}")
        log(f"     got:      {predicted}")


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
        "eval": cmd_eval,
    }

    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
