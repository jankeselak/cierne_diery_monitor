"""Magic link authentication."""

import secrets
from datetime import datetime, timedelta

import db

TOKEN_EXPIRY_DAYS = 30


def generate_token():
    """Generate a secure random token."""
    return secrets.token_urlsafe(32)


def send_magic_link(email, base_url):
    """Create or update user with a new token and send the magic link email.

    Returns the token (for testing) or None on email failure.
    """
    import notifier

    conn = db.get_connection()
    token = generate_token()
    expires = (datetime.now() + timedelta(days=TOKEN_EXPIRY_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Upsert user
    existing = conn.execute(
        "SELECT id FROM users WHERE email = ?", (email,)
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE users SET token = ?, token_expires_at = ? WHERE email = ?",
            (token, expires, email),
        )
    else:
        conn.execute(
            "INSERT INTO users (email, token, token_expires_at) VALUES (?, ?, ?)",
            (email, token, expires),
        )
    conn.commit()

    # Send email — link goes to frontend which calls the verify API
    link = f"{base_url}/?token={token}"
    success = notifier.send_magic_link_email(email, link)
    conn.close()
    return token if success else None


def verify_token(token):
    """Verify a token and return the user row, or None if invalid/expired."""
    conn = db.get_connection()
    user = conn.execute(
        "SELECT * FROM users WHERE token = ?", (token,)
    ).fetchone()

    if not user:
        conn.close()
        return None

    # Check expiry
    expires = datetime.strptime(user["token_expires_at"], "%Y-%m-%d %H:%M:%S")
    if datetime.now() > expires:
        conn.close()
        return None

    # Refresh expiry on use
    new_expires = (datetime.now() + timedelta(days=TOKEN_EXPIRY_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn.execute(
        "UPDATE users SET token_expires_at = ? WHERE id = ?",
        (new_expires, user["id"]),
    )
    conn.commit()
    conn.close()
    return dict(user)


def get_user_by_token(token):
    """Get user by token without refreshing. Returns dict or None."""
    if not token:
        return None
    conn = db.get_connection()
    user = conn.execute(
        "SELECT * FROM users WHERE token = ?", (token,)
    ).fetchone()
    conn.close()

    if not user:
        return None

    expires = datetime.strptime(user["token_expires_at"], "%Y-%m-%d %H:%M:%S")
    if datetime.now() > expires:
        return None

    return dict(user)
