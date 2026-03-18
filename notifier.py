"""Email notifications via Resend."""

import os
import resend

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Čierne Diery <ciernediery@resend.dev>")


def _init():
    resend.api_key = RESEND_API_KEY


def send_magic_link_email(to_email, link):
    """Send a magic link login email. Returns True on success."""
    _init()
    try:
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": "Prihlásenie — Čierne Diery Monitor",
            "html": f"""
            <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
                <h2 style="color: #e8175d;">Čierne Diery Monitor</h2>
                <p>Klikni na tlačidlo nižšie pre prihlásenie:</p>
                <a href="{link}"
                   style="display: inline-block; background: #e8175d; color: #fff;
                          padding: 12px 24px; text-decoration: none; font-weight: 600;
                          margin: 16px 0;">
                    Prihlásiť sa
                </a>
                <p style="color: #888; font-size: 13px;">
                    Ak si nepožiadal/a o prihlásenie, tento email ignoruj.
                </p>
                <p style="color: #888; font-size: 13px;">
                    Odkaz je platný 30 dní.
                </p>
            </div>
            """,
        })
        return True
    except Exception as e:
        print(f"[notifier] Magic link email failed: {e}")
        return False


def send_watchdog_alert(to_email, listing, catalog_item):
    """Send a watchdog alert for a new listing. Returns True on success."""
    _init()

    price_str = f"{listing['price']:.0f}€" if listing.get("price") else "Dohodou"
    image_html = ""
    if catalog_item.get("image_url"):
        image_html = f'<img src="{catalog_item["image_url"]}" alt="{catalog_item.get("building", "")}" style="max-width: 100%; height: auto; margin-bottom: 16px;">'

    building = catalog_item.get("building") or catalog_item.get("title") or ""
    artist = catalog_item.get("artist") or ""

    try:
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": f"Nový inzerát: {building} — {price_str}",
            "html": f"""
            <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
                <h2 style="color: #e8175d;">Nový inzerát na bazos.sk</h2>
                {image_html}
                <h3 style="margin: 0 0 4px;">{building}</h3>
                {"<p style='color: #666; margin: 0 0 12px;'>" + artist + "</p>" if artist else ""}
                <table style="width: 100%; font-size: 14px; margin-bottom: 16px;">
                    <tr>
                        <td style="padding: 4px 0; color: #888;">Cena</td>
                        <td style="padding: 4px 0; font-weight: 600; color: #e8175d;">{price_str}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0; color: #888;">Lokalita</td>
                        <td style="padding: 4px 0;">{listing.get("location", "—")}</td>
                    </tr>
                    <tr>
                        <td style="padding: 4px 0; color: #888;">Dátum</td>
                        <td style="padding: 4px 0;">{listing.get("date_posted", "—")}</td>
                    </tr>
                </table>
                <a href="{listing.get("url", "#")}"
                   style="display: inline-block; background: #e8175d; color: #fff;
                          padding: 12px 24px; text-decoration: none; font-weight: 600;">
                    Zobraziť inzerát
                </a>
                <p style="color: #aaa; font-size: 12px; margin-top: 24px;">
                    Čierne Diery Monitor — watchdog notifikácia
                </p>
            </div>
            """,
        })
        return True
    except Exception as e:
        print(f"[notifier] Watchdog alert failed for {to_email}: {e}")
        return False


def send_watchdog_alerts(conn, new_listings):
    """Check all new listings against watchlists and send alerts.

    Args:
        conn: database connection
        new_listings: list of listing dicts that are newly seen
    """
    if not new_listings or not RESEND_API_KEY:
        return

    sent_count = 0
    for listing in new_listings:
        catalog_id = listing.get("catalog_id")
        if not catalog_id:
            continue

        # Find watchers for this catalog item
        watchers = conn.execute("""
            SELECT w.*, u.email FROM watchlist w
            JOIN users u ON w.user_id = u.id
            WHERE w.catalog_id = ?
        """, (catalog_id,)).fetchall()

        if not watchers:
            continue

        # Get catalog item details
        catalog_item = conn.execute(
            "SELECT * FROM catalog WHERE id = ?", (catalog_id,)
        ).fetchone()
        if not catalog_item:
            continue
        catalog_item = dict(catalog_item)

        for watcher in watchers:
            # Apply price filter
            if watcher["max_price"] and listing.get("price"):
                if listing["price"] > watcher["max_price"]:
                    continue

            # Check if already notified
            already = conn.execute(
                "SELECT id FROM notifications_sent WHERE user_id = ? AND bazos_id = ?",
                (watcher["user_id"], listing["bazos_id"]),
            ).fetchone()
            if already:
                continue

            # Send alert
            if send_watchdog_alert(watcher["email"], listing, catalog_item):
                conn.execute(
                    "INSERT INTO notifications_sent (user_id, bazos_id) VALUES (?, ?)",
                    (watcher["user_id"], listing["bazos_id"]),
                )
                sent_count += 1

    if sent_count:
        conn.commit()
        print(f"[notifier] Sent {sent_count} watchdog alert(s)")
