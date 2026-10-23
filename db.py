import sqlite3
import os
from pathlib import Path

import reposts

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
            is_buying INTEGER DEFAULT 0,
            is_trading INTEGER DEFAULT 0,
            FOREIGN KEY (catalog_id) REFERENCES catalog(id)
        );

        CREATE INDEX IF NOT EXISTS idx_listings_catalog ON listings(catalog_id);
        CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price);
        CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            token TEXT UNIQUE,
            token_expires_at TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            catalog_id INTEGER NOT NULL,
            max_price REAL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (catalog_id) REFERENCES catalog(id),
            UNIQUE(user_id, catalog_id)
        );

        CREATE TABLE IF NOT EXISTS notifications_sent (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            bazos_id TEXT NOT NULL,
            sent_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, bazos_id)
        );

        CREATE TABLE IF NOT EXISTS collection (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            catalog_id INTEGER NOT NULL,
            purchase_price REAL,
            added_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (catalog_id) REFERENCES catalog(id),
            UNIQUE(user_id, catalog_id)
        );

        -- Manual match decisions per ad group; the matcher never overrides them
        CREATE TABLE IF NOT EXISTS match_overrides (
            ad_group_id TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK (status IN ('matched', 'lot', 'not_cd')),
            catalog_id INTEGER,
            user_id INTEGER,
            decided_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (catalog_id) REFERENCES catalog(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist(user_id);
        CREATE INDEX IF NOT EXISTS idx_watchlist_catalog ON watchlist(catalog_id);
        CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications_sent(user_id);
        CREATE INDEX IF NOT EXISTS idx_collection_user ON collection(user_id);
        CREATE INDEX IF NOT EXISTS idx_collection_catalog ON collection(catalog_id);
    """)
    # Migrate: add image_url column if missing
    cols = [r[1] for r in conn.execute("PRAGMA table_info(catalog)").fetchall()]
    if "image_url" not in cols:
        conn.execute("ALTER TABLE catalog ADD COLUMN image_url TEXT")
    # Migrate: add is_buying column if missing
    listing_cols = [r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()]
    if "is_buying" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN is_buying INTEGER DEFAULT 0")
        # Backfill existing buy-intent listings
        conn.execute("""
            UPDATE listings SET is_buying = 1
            WHERE LOWER(title) LIKE '%kúpim%'
               OR LOWER(title) LIKE '%kupim%'
               OR LOWER(title) LIKE '%hľadám%'
               OR LOWER(title) LIKE '%hladam%'
               OR LOWER(title) LIKE '%zoženiem%'
               OR LOWER(title) LIKE '%zozeniam%'
               OR LOWER(title) LIKE '%dopyt%'
               OR LOWER(title) LIKE '%zháňam%'
               OR LOWER(title) LIKE '%zhanam%'
        """)
    # Migrate: add is_trading column if missing
    if "is_trading" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN is_trading INTEGER DEFAULT 0")
        # Backfill existing trade-intent listings
        conn.execute("""
            UPDATE listings SET is_trading = 1
            WHERE LOWER(title) LIKE '%výmena%'
               OR LOWER(title) LIKE '%vymena%'
               OR LOWER(title) LIKE '%vymením%'
               OR LOWER(title) LIKE '%vymenim%'
               OR LOWER(title) LIKE '%vymeníme%'
               OR LOWER(title) LIKE '%vymenime%'
               OR LOWER(title) LIKE '%menujem%'
               OR LOWER(title) LIKE '%menujeme%'
               OR LOWER(title) LIKE '%vymieňam%'
               OR LOWER(title) LIKE '%vymienam%'
        """)
    # Migrate: add bundle columns if missing
    if "is_bundle" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN is_bundle INTEGER DEFAULT 0")
    if "parent_bazos_id" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN parent_bazos_id TEXT DEFAULT NULL")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_parent ON listings(parent_bazos_id)")
    # Migrate: matcher confidence per listing (1.0 = manual decision)
    if "match_confidence" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN match_confidence REAL")
    # Migrate: group reposts of the same ad. Recomputed for all rows (cheap) so
    # changes to the grouping rule apply to existing data too.
    if "ad_group_id" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN ad_group_id TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_group ON listings(ad_group_id)")
    rows = conn.execute("SELECT * FROM listings").fetchall()
    conn.executemany(
        "UPDATE listings SET ad_group_id = ? WHERE id = ? AND ad_group_id IS NOT ?",
        [(group, row["id"], group) for row in rows for group in [reposts.ad_group_id(dict(row))]],
    )
    # One row per ad: the latest repost, plus when the ad was first seen
    conn.executescript("""
        DROP VIEW IF EXISTS ads;
        CREATE VIEW ads AS
            SELECT l.*, g.group_first_seen, g.repost_count
            FROM listings l
            JOIN (SELECT ad_group_id, MAX(id) AS latest_id,
                         MIN(first_seen) AS group_first_seen, COUNT(*) AS repost_count
                  FROM listings GROUP BY ad_group_id) g
              ON l.id = g.latest_id;
    """)
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

    is_buying = listing.get("is_buying", 0)
    is_trading = listing.get("is_trading", 0)
    is_bundle = listing.get("is_bundle", 0)
    parent_bazos_id = listing.get("parent_bazos_id")
    ad_group_id = reposts.ad_group_id(listing)

    if existing:
        conn.execute("""
            UPDATE listings SET title=?, price=?, price_text=?, description=?,
                date_posted=?, location=?, postal_code=?, url=?, image_url=?,
                views=?, catalog_id=?, last_seen=datetime('now'), is_active=1,
                is_buying=?, is_trading=?, is_bundle=?, parent_bazos_id=?, ad_group_id=?,
                match_confidence=?
            WHERE bazos_id=?
        """, (
            listing["title"], listing["price"], listing["price_text"],
            listing["description"], listing["date_posted"], listing["location"],
            listing["postal_code"], listing["url"], listing["image_url"],
            listing["views"], listing["catalog_id"], is_buying, is_trading,
            is_bundle, parent_bazos_id, ad_group_id, listing.get("match_confidence"),
            listing["bazos_id"]
        ))
        return existing["id"]
    else:
        cur = conn.execute("""
            INSERT INTO listings (bazos_id, title, price, price_text, description,
                date_posted, location, postal_code, url, image_url, views, catalog_id,
                is_buying, is_trading, is_bundle, parent_bazos_id, ad_group_id,
                match_confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            listing["bazos_id"], listing["title"], listing["price"],
            listing["price_text"], listing["description"], listing["date_posted"],
            listing["location"], listing["postal_code"], listing["url"],
            listing["image_url"], listing["views"], listing["catalog_id"],
            is_buying, is_trading, is_bundle, parent_bazos_id, ad_group_id,
            listing.get("match_confidence")
        ))
        return cur.lastrowid


def delete_stale_bundle_children(conn, parent_bazos_id, current_child_ids):
    """Delete bundle children that no longer exist (items removed from listing)."""
    if not current_child_ids:
        conn.execute(
            "DELETE FROM listings WHERE parent_bazos_id = ?",
            (parent_bazos_id,)
        )
        return
    placeholders = ",".join("?" * len(current_child_ids))
    conn.execute(
        f"DELETE FROM listings WHERE parent_bazos_id = ? AND bazos_id NOT IN ({placeholders})",
        [parent_bazos_id] + list(current_child_ids)
    )


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
    where = "WHERE (is_bundle = 0 OR is_bundle IS NULL)" + (" AND is_active = 1" if active_only else "")
    return conn.execute(f"""
        SELECT l.*, c.title as catalog_title, c.building, c.artist, c.category
        FROM ads l
        LEFT JOIN catalog c ON l.catalog_id = c.id
        {where}
        ORDER BY l.date_posted DESC
    """).fetchall()


def get_price_history(conn, catalog_id):
    """Get all listing prices for a catalog item."""
    return conn.execute("""
        SELECT price, date_posted, is_active, url
        FROM ads
        WHERE catalog_id = ? AND price IS NOT NULL
        ORDER BY date_posted DESC
    """, (catalog_id,)).fetchall()


def get_price_stats(conn):
    """Get median and count per catalog item (excludes buy-intent)."""
    rows = conn.execute("""
        SELECT c.id, c.title, c.building, c.artist, c.category,
            COUNT(l.id) as listing_count,
            MIN(l.price) as min_price,
            MAX(l.price) as max_price,
            AVG(l.price) as avg_price
        FROM catalog c
        JOIN ads l ON l.catalog_id = c.id
        WHERE l.price IS NOT NULL AND l.is_buying = 0
        GROUP BY c.id
        HAVING listing_count >= 1
        ORDER BY listing_count DESC
    """).fetchall()
    return rows


# Matches below this confidence are shown in the review queue
REVIEW_CONFIDENCE = 0.85


def get_overrides(conn):
    """Manual match decisions by ad group."""
    return {row["ad_group_id"]: dict(row) for row in conn.execute("SELECT * FROM match_overrides")}


def apply_overrides(listings, overrides):
    """Replace matcher results with manual decisions (in place)."""
    for listing in listings:
        override = overrides.get(reposts.ad_group_id(listing))
        if override:
            listing["catalog_id"] = override["catalog_id"] if override["status"] == "matched" else None
            listing["match_confidence"] = 1.0


def set_override(conn, ad_group_id, status, catalog_id=None, user_id=None):
    """Save a manual decision and apply it to every listing of the ad group."""
    catalog_id = catalog_id if status == "matched" else None
    conn.execute("""
        INSERT INTO match_overrides (ad_group_id, status, catalog_id, user_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ad_group_id) DO UPDATE SET status = excluded.status,
            catalog_id = excluded.catalog_id, user_id = excluded.user_id,
            decided_at = datetime('now')
    """, (ad_group_id, status, catalog_id, user_id))
    conn.execute(
        "UPDATE listings SET catalog_id = ?, match_confidence = 1.0 WHERE ad_group_id = ?",
        (catalog_id, ad_group_id),
    )
    conn.commit()


def get_review_queue(conn):
    """Active ads that are unmatched or low-confidence and not reviewed yet, newest first."""
    return conn.execute("""
        SELECT a.ad_group_id, a.bazos_id, a.title, a.description, a.price, a.price_text,
               a.url, a.image_url, a.location, a.date_posted, a.is_buying, a.is_trading,
               a.catalog_id, a.match_confidence, a.repost_count,
               c.title AS guess_title, c.archive_slug AS guess_slug
        FROM ads a
        LEFT JOIN catalog c ON c.id = a.catalog_id
        WHERE a.is_active = 1
          AND a.parent_bazos_id IS NULL
          AND (a.is_bundle = 0 OR a.is_bundle IS NULL)
          AND a.ad_group_id NOT IN (SELECT ad_group_id FROM match_overrides)
          AND (a.catalog_id IS NULL OR COALESCE(a.match_confidence, 0) < ?)
        ORDER BY a.group_first_seen DESC
    """, (REVIEW_CONFIDENCE,)).fetchall()
