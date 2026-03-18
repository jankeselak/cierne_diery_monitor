#!/usr/bin/env python3
"""Web server for Čierne diery deal finder."""

import json
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from statistics import median as calc_median
from urllib.parse import urlparse, parse_qs

import db
import analyzer
import auth

STATIC_DIR = Path(__file__).parent / "static"
PORT = 8080


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
            ("POST", "/api/auth/login"): self._api_auth_login,
            ("GET", "/api/auth/verify"): self._api_auth_verify,
            ("GET", "/api/auth/me"): self._api_auth_me,
            ("GET", "/api/watchlist"): self._api_watchlist_get,
            ("POST", "/api/watchlist"): self._api_watchlist_add,
            ("DELETE", "/api/watchlist"): self._api_watchlist_remove,
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
                 WHERE l.catalog_id = c.id AND l.is_active = 1) as active_count
            FROM catalog c
            ORDER BY c.title
        """).fetchall()
        # Attach active listings per catalog item
        result = []
        for item in catalog:
            d = dict(item)
            if d["active_count"] > 0:
                ads = conn.execute("""
                    SELECT bazos_id, title, price, price_text, date_posted,
                           location, url, views
                    FROM listings
                    WHERE catalog_id = ? AND is_active = 1
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
        d = dict(item)
        # All listings for this item (active + inactive), newest first
        all_listings = conn.execute("""
            SELECT bazos_id, title, price, price_text, date_posted,
                   location, url, views, is_active, first_seen, last_seen
            FROM listings
            WHERE catalog_id = ?
            ORDER BY date_posted DESC
        """, (item_id,)).fetchall()
        d["all_listings"] = [dict(l) for l in all_listings]
        # Price stats
        prices = [l["price"] for l in all_listings if l["price"]]
        if prices:
            d["price_stats"] = {
                "count": len(prices),
                "min": min(prices),
                "max": max(prices),
                "median": calc_median(prices),
                "avg": sum(prices) / len(prices),
                "prices": sorted(prices),
            }
        else:
            d["price_stats"] = None
        # Price snapshots over time
        snapshots = db.get_snapshots(conn, item_id)
        d["snapshots"] = [dict(s) for s in snapshots]
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

    def log_message(self, format, *args):
        # Suppress request logs for cleaner output
        pass


def main():
    db.init_db()
    STATIC_DIR.mkdir(exist_ok=True)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    server = HTTPServer(("", port), Handler)
    print(f"ČIERNE DIERY MONITOR → http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
