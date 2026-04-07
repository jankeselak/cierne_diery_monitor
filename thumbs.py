"""Download and resize catalog images to thumbnails for faster page loads."""

import io
import requests
from pathlib import Path
from PIL import Image

THUMB_DIR = Path(__file__).parent / "static" / "img"
THUMB_WIDTH = 300
JPEG_QUALITY = 75
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def ensure_thumbnails(catalog_items, on_progress=None):
    """Download and resize thumbnails for catalog items that don't have one yet."""
    THUMB_DIR.mkdir(parents=True, exist_ok=True)

    created = 0
    for item in catalog_items:
        slug = item.get("archive_slug")
        image_url = item.get("image_url")
        if not slug or not image_url:
            continue

        thumb_path = THUMB_DIR / f"{slug}.jpg"
        if thumb_path.exists():
            continue

        try:
            resp = requests.get(image_url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content))
            img = img.convert("RGB")

            # Resize to THUMB_WIDTH, maintaining aspect ratio
            ratio = THUMB_WIDTH / img.width
            new_height = int(img.height * ratio)
            img = img.resize((THUMB_WIDTH, new_height), Image.LANCZOS)

            img.save(thumb_path, "JPEG", quality=JPEG_QUALITY, optimize=True)
            created += 1
        except Exception:
            continue

    if on_progress:
        on_progress(f"Thumbnails: {created} new, {len(list(THUMB_DIR.glob('*.jpg')))} total")
