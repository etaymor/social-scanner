"""Visual OCR — extract on-screen text from post cover images via Gemini Flash."""

import base64
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config

from . import db

log = logging.getLogger(__name__)

_OCR_PROMPT = """\
Read ALL text visible on screen in this social media post image.
Return ONLY the on-screen text, exactly as it appears, one line per text element.
Include place names, addresses, numbers/rankings, and any overlaid captions.
If there is no readable text, return "NO_TEXT"."""


def _download_image(url: str, timeout: int = 10) -> bytes | None:
    """Download an image and return raw bytes, or None on failure."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException:
        return None


def _ocr_image(image_bytes: bytes) -> str | None:
    """Send image to Gemini Flash via OpenRouter and return extracted text."""
    b64 = base64.b64encode(image_bytes).decode("utf-8")

    payload = {
        "model": "google/gemini-2.0-flash-001",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                    {"type": "text", "text": _OCR_PROMPT},
                ],
            }
        ],
        "temperature": 0.1,
        "max_tokens": 500,
    }

    headers = {
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            config.OPENROUTER_BASE_URL,
            json=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        if text == "NO_TEXT" or not text:
            return None
        return text
    except (requests.RequestException, KeyError, IndexError):
        log.debug("OCR request failed", exc_info=True)
        return None


def _process_one(post_id: int, cover_url: str) -> tuple[int, str | None]:
    """Download + OCR a single cover image. Returns (post_id, extracted_text)."""
    image_bytes = _download_image(cover_url)
    if not image_bytes:
        return post_id, None
    text = _ocr_image(image_bytes)
    return post_id, text


def extract_cover_text(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    batch_size: int = 20,
) -> int:
    """OCR cover images for posts that have a cover_url but haven't been OCR'd yet.

    Appends extracted on-screen text to the post's caption so the place
    extractor can use it. Returns the number of posts enriched.
    """
    posts = conn.execute(
        """SELECT id, cover_url, caption FROM raw_posts
           WHERE city_id = ? AND cover_url IS NOT NULL AND cover_url != ''
                 AND processed = FALSE
           LIMIT ?""",
        (city_id, batch_size),
    ).fetchall()

    if not posts:
        log.info("No posts with cover images to OCR for %s", city_name)
        return 0

    log.info("Running visual OCR on %d cover images for %s...", len(posts), city_name)
    enriched = 0

    # Process in parallel (limited concurrency to avoid rate limits)
    max_workers = min(5, len(posts))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_process_one, post["id"], post["cover_url"]): post
            for post in posts
        }

        for future in as_completed(futures):
            post = futures[future]
            try:
                post_id, ocr_text = future.result()
                if ocr_text:
                    # Append OCR text to existing caption
                    existing = post["caption"] or ""
                    updated = existing + f"\n🔤 On-screen text: {ocr_text}"
                    conn.execute(
                        "UPDATE raw_posts SET caption = ? WHERE id = ?",
                        (updated, post_id),
                    )
                    enriched += 1
                    log.debug("OCR enriched post %d: %s", post_id, ocr_text[:100])
            except Exception:
                log.debug("OCR failed for post %d", post["id"], exc_info=True)

    conn.commit()
    log.info("Visual OCR complete for %s: %d/%d posts enriched", city_name, enriched, len(posts))
    return enriched
