"""Visual OCR — extract on-screen text from post cover/slideshow images."""

from __future__ import annotations

import base64
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config

log = logging.getLogger(__name__)

_OCR_PROMPT = """\
Read ALL text visible on screen in this social media post image.
Return ONLY the on-screen text, exactly as it appears, one line per text element.
Include place names, addresses, numbers/rankings, and any overlaid captions.
If there is no readable text, return "NO_TEXT"."""

# Fail closed when this fraction of OCR HTTP attempts fail (model/API down).
_FAIL_CLOSED_RATIO = 0.5
_MIN_ATTEMPTS_FOR_FAIL_CLOSED = 5


class OCRError(Exception):
    """Raised when OCR is systemically unavailable (fail closed)."""


class _OCRAttempt:
    __slots__ = ("post_id", "text", "http_error", "download_error")

    def __init__(
        self,
        post_id: int,
        text: str | None = None,
        *,
        http_error: bool = False,
        download_error: bool = False,
    ) -> None:
        self.post_id = post_id
        self.text = text
        self.http_error = http_error
        self.download_error = download_error


def _download_image(url: str, timeout: int = 10) -> tuple[bytes | None, bool]:
    """Return (bytes, download_error). download_error True on HTTP/network failure."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content, False
    except requests.RequestException:
        return None, True


def _ocr_image(image_bytes: bytes) -> tuple[str | None, bool]:
    """Return (text, http_error). http_error True on API/transport failure incl. 404."""
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    model = getattr(config, "OCR_MODEL", None) or "google/gemini-2.0-flash-001"

    payload = {
        "model": model,
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
        if resp.status_code == 404:
            log.error("OCR model/API returned 404 — extractor unavailable")
            return None, True
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        if text == "NO_TEXT" or not text:
            return None, False
        return text, False
    except (requests.RequestException, KeyError, IndexError, TypeError):
        log.debug("OCR request failed", exc_info=True)
        return None, True


def _process_one(post_id: int, image_urls: list[str]) -> _OCRAttempt:
    """Download + OCR one or more images for a post."""
    texts: list[str] = []
    any_http = False
    any_download = False
    attempted = False

    for url in image_urls:
        if not url:
            continue
        attempted = True
        image_bytes, dl_err = _download_image(url)
        if dl_err or not image_bytes:
            any_download = True
            continue
        text, http_err = _ocr_image(image_bytes)
        if http_err:
            any_http = True
            continue
        if text:
            texts.append(text)

    if not attempted:
        return _OCRAttempt(post_id, download_error=True)

    combined = "\n".join(texts) if texts else None
    return _OCRAttempt(
        post_id,
        combined,
        http_error=any_http and not combined,
        download_error=any_download and not combined and not any_http,
    )


def _image_urls_for_post(post: sqlite3.Row) -> list[str]:
    urls: list[str] = []
    cover = post["cover_url"] if "cover_url" in post.keys() else None
    if cover:
        urls.append(cover)
    raw = post["slideshow_urls"] if "slideshow_urls" in post.keys() else None
    if raw:
        for part in str(raw).split("\n"):
            part = part.strip()
            if part and part not in urls:
                urls.append(part)
    return urls


def extract_cover_text(
    conn: sqlite3.Connection,
    city_id: int,
    city_name: str,
    batch_size: int = 50,
    *,
    fail_closed: bool = True,
) -> int:
    """OCR cover/slideshow images for posts not yet OCR'd.

    Loops until no pending posts remain (unlike the old single LIMIT-20 pass).
    Appends on-screen text to captions. Raises OCRError when the OCR API is
    systemically down and fail_closed is True — so we never silently ship a
    captions-only 4-place list.
    """
    total_enriched = 0
    total_attempted = 0
    total_http_errors = 0

    while True:
        posts = conn.execute(
            """SELECT id, cover_url, slideshow_urls, caption FROM raw_posts
               WHERE city_id = ?
                 AND processed = FALSE
                 AND COALESCE(ocr_status, 'pending') = 'pending'
                 AND (
                   (cover_url IS NOT NULL AND cover_url != '')
                   OR (slideshow_urls IS NOT NULL AND slideshow_urls != '')
                 )
               LIMIT ?""",
            (city_id, batch_size),
        ).fetchall()

        if not posts:
            break

        log.info("Running visual OCR on %d images for %s...", len(posts), city_name)
        max_workers = min(5, len(posts))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_process_one, post["id"], _image_urls_for_post(post)): post
                for post in posts
            }
            for future in as_completed(futures):
                post = futures[future]
                total_attempted += 1
                try:
                    result = future.result()
                except Exception:
                    total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
                        (post["id"],),
                    )
                    log.debug("OCR failed for post %d", post["id"], exc_info=True)
                    continue

                if result.http_error:
                    total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = 'failed' WHERE id = ?",
                        (post["id"],),
                    )
                    continue

                if result.text:
                    existing = post["caption"] or ""
                    if "🔤 On-screen text:" not in existing:
                        updated = existing + f"\n🔤 On-screen text: {result.text}"
                        conn.execute(
                            "UPDATE raw_posts SET caption = ?, ocr_status = 'done' WHERE id = ?",
                            (updated, result.post_id),
                        )
                    else:
                        conn.execute(
                            "UPDATE raw_posts SET ocr_status = 'done' WHERE id = ?",
                            (result.post_id,),
                        )
                    total_enriched += 1
                else:
                    # No text or download-only miss — mark done so we don't spin forever
                    status = "empty" if not result.download_error else "failed"
                    if result.download_error:
                        total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = ? WHERE id = ?",
                        (status, result.post_id),
                    )

        conn.commit()

    if (
        fail_closed
        and total_attempted >= _MIN_ATTEMPTS_FOR_FAIL_CLOSED
        and total_http_errors / total_attempted >= _FAIL_CLOSED_RATIO
    ):
        raise OCRError(
            f"OCR failed for {total_http_errors}/{total_attempted} posts in {city_name}. "
            "Refusing to continue with captions-only extraction (fail closed). "
            "Fix OCR credentials/model or re-run with --skip-ocr after acknowledging "
            "on-screen venue names will be missing."
        )

    if total_attempted == 0:
        log.info("No posts with cover/slideshow images to OCR for %s", city_name)
    else:
        log.info(
            "Visual OCR complete for %s: %d enriched, %d attempted, %d errors",
            city_name,
            total_enriched,
            total_attempted,
            total_http_errors,
        )
    return total_enriched
