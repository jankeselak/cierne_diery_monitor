import sqlite3
import os
from pathlib import Path

DB_DIR = Path(__file__).parent / "data"
DB_PATH = DB_DIR / "listings.db"


def get_connection():
    DB_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS catalog (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            building TEXT,
            artist TEXT,
            location TEXT,
            archive_url TEXT,
            archive_slug TEXT UNIQUE,
            image_url TEXT,
            category TEXT,
            retail_price REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY,
            bazos_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            price REAL,
            price_text TEXT,
            description TEXT,
            date_posted TEXT,
            location TEXT,
            postal_code TEXT,
            url TEXT,
            image_url TEXT,
            views INTEGER,
            catalog_id INTEGER,
            first_seen TEXT DEFAULT (datetime('now')),
            last_seen TEXT DEFAULT (datetime('now')),
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (catalog_id) REFERENCES catalog(id)
        );

        CREATE INDEX IF NOT EXISTS idx_listings_catalog ON listings(catalog_id);
        CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price);
        CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY,
            catalog_id INTEGER NOT NULL,
            scraped_at TEXT NOT NULL,
            min_price REAL,
            max_price REAL,
            avg_price REAL,
            median_price REAL,
            listing_count INTEGER,
            FOREIGN KEY (catalog_id) REFERENCES catalog(id)
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_catalog ON price_snapshots(catalog_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_date ON price_snapshots(scraped_at);
    """)
    # Migrate: add image_url column if missing
    cols = [r[1] for r in conn.execute("PRAGMA table_info(catalog)").fetchall()]
    if "image_url" not in cols:
        conn.execute("ALTER TABLE catalog ADD COLUMN image_url TEXT")
    conn.commit()
    conn.close()


def upsert_catalog_item(conn, item):
    """Insert or update a catalog item. Returns the row id."""
    existing = conn.execute(
        "SELECT id FROM catalog WHERE archive_slug = ?",
        (item["archive_slug"],)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE catalog SET title=?, building=?, artist=?, location=?,
                archive_url=?, image_url=?, category=?
            WHERE archive_slug=?
        """, (
            item["title"], item["building"], item["artist"], item["location"],
            item["archive_url"], item.get("image_url"), item["category"],
            item["archive_slug"]
        ))
        return existing["id"]
    else:
        cur = conn.execute("""
            INSERT INTO catalog (title, building, artist, location,
                archive_url, archive_slug, image_url, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item["title"], item["building"], item["artist"], item["location"],
            item["archive_url"], item["archive_slug"], item.get("image_url"),
            item["category"]
        ))
        return cur.lastrowid


def upsert_listing(conn, listing):
    """Insert or update a bazos listing. Returns the row id."""
    existing = conn.execute(
        "SELECT id FROM listings WHERE bazos_id = ?",
        (listing["bazos_id"],)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE listings SET title=?, price=?, price_text=?, description=?,
                date_posted=?, location=?, postal_code=?, url=?, image_url=?,
                views=?, catalog_id=?, last_seen=datetime('now'), is_active=1
            WHERE bazos_id=?
        """, (
            listing["title"], listing["price"], listing["price_text"],
            listing["description"], listing["date_posted"], listing["location"],
            listing["postal_code"], listing["url"], listing["image_url"],
            listing["views"], listing["catalog_id"], listing["bazos_id"]
        ))
        return existing["id"]
    else:
        cur = conn.execute("""
            INSERT INTO listings (bazos_id, title, price, price_text, description,
                date_posted, location, postal_code, url, image_url, views, catalog_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            listing["bazos_id"], listing["title"], listing["price"],
            listing["price_text"], listing["description"], listing["date_posted"],
            listing["location"], listing["postal_code"], listing["url"],
            listing["image_url"], listing["views"], listing["catalog_id"]
        ))
        return cur.lastrowid


def mark_inactive(conn, active_bazos_ids):
    """Mark listings not in the active set as inactive."""
    if not active_bazos_ids:
        return
    placeholders = ",".join("?" * len(active_bazos_ids))
    conn.execute(f"""
        UPDATE listings SET is_active = 0
        WHERE bazos_id NOT IN ({placeholders}) AND is_active = 1
    """, list(active_bazos_ids))


def get_catalog(conn):
    return conn.execute("SELECT * FROM catalog ORDER BY title").fetchall()


def get_listings(conn, active_only=True):
    where = "WHERE is_active = 1" if active_only else ""
    return conn.execute(f"""
        SELECT l.*, c.title as catalog_title, c.building, c.artist, c.category
        FROM listings l
        LEFT JOIN catalog c ON l.catalog_id = c.id
        {where}
        ORDER BY l.date_posted DESC
    """).fetchall()


def get_price_history(conn, catalog_id):
    """Get all listing prices for a catalog item."""
    return conn.execute("""
        SELECT price, date_posted, is_active, url
        FROM listings
        WHERE catalog_id = ? AND price IS NOT NULL
        ORDER BY date_posted DESC
    """, (catalog_id,)).fetchall()


def record_snapshots(conn):
    """Record current market price ranges per catalog item. Call after each scrape."""
    from statistics import median as calc_median
    from datetime import datetime

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Get active listing prices grouped by catalog item
    rows = conn.execute("""
        SELECT catalog_id, GROUP_CONCAT(price) as prices
        FROM listings
        WHERE catalog_id IS NOT NULL AND price IS NOT NULL AND is_active = 1
        GROUP BY catalog_id
    """).fetchall()

    for row in rows:
        prices = [float(p) for p in row["prices"].split(",")]
        conn.execute("""
            INSERT INTO price_snapshots
                (catalog_id, scraped_at, min_price, max_price, avg_price,
                 median_price, listing_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row["catalog_id"], now, min(prices), max(prices),
            sum(prices) / len(prices), calc_median(prices), len(prices)
        ))


def get_snapshots(conn, catalog_id):
    """Get all price snapshots for a catalog item, ordered by date."""
    return conn.execute("""
        SELECT scraped_at, min_price, max_price, avg_price,
               median_price, listing_count
        FROM price_snapshots
        WHERE catalog_id = ?
        ORDER BY scraped_at ASC
    """, (catalog_id,)).fetchall()


def get_price_stats(conn):
    """Get median and count per catalog item."""
    rows = conn.execute("""
        SELECT c.id, c.title, c.building, c.artist, c.category,
            COUNT(l.id) as listing_count,
            MIN(l.price) as min_price,
            MAX(l.price) as max_price,
            AVG(l.price) as avg_price
        FROM catalog c
        JOIN listings l ON l.catalog_id = c.id
        WHERE l.price IS NOT NULL
        GROUP BY c.id
        HAVING listing_count >= 1
        ORDER BY listing_count DESC
    """).fetchall()
    return rows
