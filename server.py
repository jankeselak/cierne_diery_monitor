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

    def _get_params(self):
        parsed = urlparse(self.path)
        return parse_qs(parsed.query)

    def _handle_api(self):
        path = self.path.split("?")[0]
        routes = {
            "/api/stats": self._api_stats,
            "/api/deals": self._api_deals,
            "/api/listings": self._api_listings,
            "/api/price-stats": self._api_price_stats,
            "/api/catalog": self._api_catalog,
            "/api/catalog-detail": self._api_catalog_detail,
        }
        handler = routes.get(path)
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
