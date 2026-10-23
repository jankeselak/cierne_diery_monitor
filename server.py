#!/usr/bin/env python3
"""Web server for Čierne diery deal finder."""

import json
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median as calc_median
from urllib.parse import urlparse, parse_qs

import db
import analyzer
import auth

STATIC_DIR = Path(__file__).parent / "static"
THUMB_DIR = STATIC_DIR / "img"
PORT = 8080


def _add_thumb_url(d):
    """Add thumb_url to a catalog dict if thumbnail exists."""
    slug = d.get("archive_slug")
    if slug and (THUMB_DIR / f"{slug}.jpg").exists():
        d["thumb_url"] = f"/img/{slug}.jpg"
    return d


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            self.send_error(404)

    def do_DELETE(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            self.send_error(404)

    def do_PATCH(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            self.send_error(404)

    def _get_params(self):
        parsed = urlparse(self.path)
        return parse_qs(parsed.query)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw)

    def _get_current_user(self):
        """Extract and verify auth token from request header."""
        token = self.headers.get("X-Auth-Token", "")
        if not token:
            return None
        return auth.get_user_by_token(token)

    def _handle_api(self):
        path = self.path.split("?")[0]
        method = self.command

        routes = {
            ("GET", "/api/stats"): self._api_stats,
            ("GET", "/api/deals"): self._api_deals,
            ("GET", "/api/listings"): self._api_listings,
            ("GET", "/api/price-stats"): self._api_price_stats,
            ("GET", "/api/catalog"): self._api_catalog,
            ("GET", "/api/catalog-detail"): self._api_catalog_detail,
            ("GET", "/api/recent"): self._api_recent,
            ("POST", "/api/auth/login"): self._api_auth_login,
            ("GET", "/api/auth/verify"): self._api_auth_verify,
            ("GET", "/api/auth/me"): self._api_auth_me,
            ("GET", "/api/watchlist"): self._api_watchlist_get,
            ("POST", "/api/watchlist"): self._api_watchlist_add,
            ("DELETE", "/api/watchlist"): self._api_watchlist_remove,
            ("GET", "/api/collection"): self._api_collection_get,
            ("POST", "/api/collection"): self._api_collection_add,
            ("DELETE", "/api/collection"): self._api_collection_remove,
            ("PATCH", "/api/collection"): self._api_collection_update,
        }
        handler = routes.get((method, path))
        if handler:
            try:
                data = handler()
                self._json_response(data)
            except Exception as e:
                self._json_response({"error": str(e)}, status=500)
        else:
            self.send_error(404)

    def _json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _api_stats(self):
        conn = db.get_connection()
        stats = analyzer.get_stats_summary(conn)
        deals = analyzer.find_deals(conn)
        great = sum(1 for d in deals if d["score"] >= 30)
        good = sum(1 for d in deals if 15 <= d["score"] < 30)
        conn.close()
        return {**stats, "great_deals": great, "good_deals": good}

    def _api_deals(self):
        conn = db.get_connection()
        deals = analyzer.find_deals(conn)
        conn.close()
        return deals

    def _api_listings(self):
        conn = db.get_connection()
        rows = db.get_listings(conn, active_only=True)
        conn.close()
        return [dict(r) for r in rows]

    def _api_catalog(self):
        conn = db.get_connection()
        catalog = conn.execute("""
            SELECT c.*,
                (SELECT COUNT(*) FROM listings l
                 WHERE l.catalog_id = c.id AND l.is_active = 1 AND l.is_buying = 0
                       AND (l.is_bundle = 0 OR l.is_bundle IS NULL)) as active_count
            FROM catalog c
            ORDER BY c.title
        """).fetchall()
        # Attach active listings and thumb URL per catalog item
        result = []
        for item in catalog:
            d = _add_thumb_url(dict(item))
            if d["active_count"] > 0:
                ads = conn.execute("""
                    SELECT bazos_id, title, price, price_text, date_posted,
                           location, url, views
                    FROM listings
                    WHERE catalog_id = ? AND is_active = 1 AND is_buying = 0
                          AND (is_bundle = 0 OR is_bundle IS NULL)
                    ORDER BY price ASC
                """, (d["id"],)).fetchall()
                d["ads"] = [dict(a) for a in ads]
            else:
                d["ads"] = []
            result.append(d)
        conn.close()
        return result

    def _api_catalog_detail(self):
        params = self._get_params()
        item_id = params.get("id", [None])[0]
        if not item_id:
            return {"error": "Missing id parameter"}
        conn = db.get_connection()
        item = conn.execute("SELECT * FROM catalog WHERE id = ?", (item_id,)).fetchone()
        if not item:
            conn.close()
            return {"error": "Not found"}
        d = _add_thumb_url(dict(item))
        # All listings for this item (active + inactive), newest first
        all_listings = conn.execute("""
            SELECT bazos_id, title, price, price_text, date_posted,
                   location, url, views, is_active, is_buying, is_trading, first_seen, last_seen
            FROM listings
            WHERE catalog_id = ?
            ORDER BY date_posted DESC
        """, (item_id,)).fetchall()
        d["all_listings"] = [dict(l) for l in all_listings]
        conn.close()
        return d

    def _api_price_stats(self):
        conn = db.get_connection()
        rows = db.get_price_stats(conn)
        result = []
        for row in rows:
            prices = db.get_price_history(conn, row["id"])
            price_list = [p["price"] for p in prices if p["price"]]
            med = calc_median(price_list) if price_list else 0
            result.append({**dict(row), "median_price": med})
        conn.close()
        return result

    def _api_recent(self):
        params = self._get_params()
        limit = int(params.get("limit", [30])[0])
        limit = min(limit, 100)
        conn = db.get_connection()
        rows = conn.execute("""
            SELECT l.bazos_id, l.title, l.price, l.price_text, l.location,
                   l.url, l.image_url, l.first_seen, l.date_posted, l.views,
                   l.catalog_id, l.is_active, l.is_buying, l.is_trading,
                   c.title as catalog_title, c.building, c.artist,
                   c.image_url as catalog_image, c.archive_slug, c.category
            FROM listings l
            LEFT JOIN catalog c ON l.catalog_id = c.id
            WHERE (l.is_bundle = 0 OR l.is_bundle IS NULL)
            ORDER BY l.first_seen DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            slug = d.pop("archive_slug", None)
            if slug and (THUMB_DIR / f"{slug}.jpg").exists():
                d["catalog_thumb"] = f"/img/{slug}.jpg"
            result.append(d)
        return result

    # ── AUTH ──

    def _api_auth_login(self):
        body = self._read_json_body()
        email = (body.get("email") or "").strip().lower()
        if not email or "@" not in email:
            return {"error": "Neplatný email"}

        # Build base URL from Host header
        host = self.headers.get("Host", f"localhost:{PORT}")
        scheme = "https" if "443" in host else "http"
        base_url = f"{scheme}://{host}"

        token = auth.send_magic_link(email, base_url)
        if token:
            return {"ok": True, "message": "Odkaz na prihlásenie bol odoslaný na tvoj email."}
        return {"error": "Nepodarilo sa odoslať email. Skús to znova."}

    def _api_auth_verify(self):
        params = self._get_params()
        token = params.get("token", [None])[0]
        if not token:
            return {"error": "Chýba token"}

        user = auth.verify_token(token)
        if not user:
            return {"error": "Neplatný alebo expirovaný odkaz"}

        return {"ok": True, "token": user["token"], "email": user["email"]}

    def _api_auth_me(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený", "authenticated": False}
        return {"authenticated": True, "email": user["email"]}

    # ── WATCHLIST ──

    def _api_watchlist_get(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        conn = db.get_connection()
        rows = conn.execute("""
            SELECT w.id, w.catalog_id, w.max_price, w.created_at,
                   c.title, c.building, c.artist, c.image_url, c.category
            FROM watchlist w
            JOIN catalog c ON w.catalog_id = c.id
            WHERE w.user_id = ?
            ORDER BY w.created_at DESC
        """, (user["id"],)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _api_watchlist_add(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        body = self._read_json_body()
        catalog_id = body.get("catalog_id")
        max_price = body.get("max_price")

        if not catalog_id:
            return {"error": "Chýba catalog_id"}

        conn = db.get_connection()
        # Check catalog item exists
        item = conn.execute(
            "SELECT id FROM catalog WHERE id = ?", (catalog_id,)
        ).fetchone()
        if not item:
            conn.close()
            return {"error": "Položka neexistuje"}

        # Upsert watch
        existing = conn.execute(
            "SELECT id FROM watchlist WHERE user_id = ? AND catalog_id = ?",
            (user["id"], catalog_id),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE watchlist SET max_price = ? WHERE id = ?",
                (max_price, existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO watchlist (user_id, catalog_id, max_price) VALUES (?, ?, ?)",
                (user["id"], catalog_id, max_price),
            )
        conn.commit()
        conn.close()
        return {"ok": True}

    def _api_watchlist_remove(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        body = self._read_json_body()
        catalog_id = body.get("catalog_id")
        if not catalog_id:
            return {"error": "Chýba catalog_id"}

        conn = db.get_connection()
        conn.execute(
            "DELETE FROM watchlist WHERE user_id = ? AND catalog_id = ?",
            (user["id"], catalog_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    # ── COLLECTION ──

    def _api_collection_get(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        conn = db.get_connection()
        rows = conn.execute("""
            SELECT col.id, col.catalog_id, col.purchase_price, col.added_at,
                   c.title, c.building, c.artist, c.image_url, c.archive_slug,
                   c.category, c.location, c.archive_url
            FROM collection col
            JOIN catalog c ON col.catalog_id = c.id
            WHERE col.user_id = ?
            ORDER BY col.added_at DESC
        """, (user["id"],)).fetchall()

        items = []
        total_value = 0
        # For return calculation: only items with BOTH value and purchase price
        return_value = 0
        return_cost = 0

        for row in rows:
            d = _add_thumb_url(dict(row))
            # Current value = median of active sell listings (exclude buy+trade)
            active_prices = conn.execute("""
                SELECT price FROM listings
                WHERE catalog_id = ? AND is_active = 1
                      AND is_buying = 0 AND (is_bundle = 0 OR is_bundle IS NULL)
                      AND price IS NOT NULL
            """, (d["catalog_id"],)).fetchall()

            if active_prices:
                prices = [r["price"] for r in active_prices]
                d["current_value"] = calc_median(prices)
            else:
                # Fallback: last known listing price
                last = conn.execute("""
                    SELECT price FROM listings
                    WHERE catalog_id = ? AND price IS NOT NULL
                          AND is_buying = 0 AND (is_bundle = 0 OR is_bundle IS NULL)
                    ORDER BY last_seen DESC LIMIT 1
                """, (d["catalog_id"],)).fetchone()
                d["current_value"] = last["price"] if last else None

            if d["current_value"] is not None:
                total_value += d["current_value"]
                # Only include in return calc if we know both sides
                if d["purchase_price"] is not None:
                    return_value += d["current_value"]
                    return_cost += d["purchase_price"]

            items.append(d)

        conn.close()

        has_return = return_cost > 0 or return_value > 0

        summary = {
            "count": len(items),
            "total_value": round(total_value, 2),
            "total_cost": round(return_cost, 2) if has_return else None,
            "return": round(return_value - return_cost, 2) if has_return else None,
        }

        return {"items": items, "summary": summary}

    def _api_collection_add(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        body = self._read_json_body()
        catalog_id = body.get("catalog_id")
        purchase_price = body.get("purchase_price")

        if not catalog_id:
            return {"error": "Chýba catalog_id"}

        conn = db.get_connection()
        item = conn.execute(
            "SELECT id FROM catalog WHERE id = ?", (catalog_id,)
        ).fetchone()
        if not item:
            conn.close()
            return {"error": "Položka neexistuje"}

        existing = conn.execute(
            "SELECT id FROM collection WHERE user_id = ? AND catalog_id = ?",
            (user["id"], catalog_id),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE collection SET purchase_price = ? WHERE id = ?",
                (purchase_price, existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO collection (user_id, catalog_id, purchase_price) VALUES (?, ?, ?)",
                (user["id"], catalog_id, purchase_price),
            )
            # Auto-watch: enable notifications when adding to collection
            watched = conn.execute(
                "SELECT id FROM watchlist WHERE user_id = ? AND catalog_id = ?",
                (user["id"], catalog_id),
            ).fetchone()
            if not watched:
                conn.execute(
                    "INSERT INTO watchlist (user_id, catalog_id) VALUES (?, ?)",
                    (user["id"], catalog_id),
                )
        conn.commit()
        conn.close()
        return {"ok": True}

    def _api_collection_remove(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        body = self._read_json_body()
        catalog_id = body.get("catalog_id")
        if not catalog_id:
            return {"error": "Chýba catalog_id"}

        conn = db.get_connection()
        conn.execute(
            "DELETE FROM collection WHERE user_id = ? AND catalog_id = ?",
            (user["id"], catalog_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    def _api_collection_update(self):
        user = self._get_current_user()
        if not user:
            return {"error": "Neprihlásený"}

        body = self._read_json_body()
        catalog_id = body.get("catalog_id")
        purchase_price = body.get("purchase_price")

        if not catalog_id:
            return {"error": "Chýba catalog_id"}

        conn = db.get_connection()
        conn.execute(
            "UPDATE collection SET purchase_price = ? WHERE user_id = ? AND catalog_id = ?",
            (purchase_price, user["id"], catalog_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}

    def log_message(self, format, *args):
        # Suppress request logs for cleaner output
        pass


def main():
    db.init_db()
    STATIC_DIR.mkdir(exist_ok=True)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    server = ThreadingHTTPServer(("", port), Handler)
    print(f"ČIERNE DIERY MONITOR → http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
