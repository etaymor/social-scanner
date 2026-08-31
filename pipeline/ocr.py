"""Visual OCR — extract on-screen text from post cover/slideshow images.

Tries a fallback chain of engines. One engine 404/timeout/auth miss must not
abort the city run — the next engine is tried. ``NO_TEXT`` from a vision model
is a successful empty read, not an HTTP failure. Never fail-closed.
"""

from __future__ import annotations

import base64
import io
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests

import config

log = logging.getLogger(__name__)

_OCR_PROMPT = """\
Read ALL text visible on screen in this social media post image.
Return ONLY the on-screen text, exactly as it appears, one line per text element.
Include place names, addresses, numbers/rankings, and any overlaid captions.
If there is no readable text, return "NO_TEXT"."""


class _OCRAttempt:
    __slots__ = ("post_id", "text", "http_error", "download_error", "engine")

    def __init__(
        self,
        post_id: int,
        text: str | None = None,
        *,
        http_error: bool = False,
        download_error: bool = False,
        engine: str | None = None,
    ) -> None:
        self.post_id = post_id
        self.text = text
        self.http_error = http_error
        self.download_error = download_error
        self.engine = engine


@dataclass(frozen=True)
class _EngineResult:
    """Outcome of a single OCR engine attempt on one image."""

    text: str | None = None
    engine_error: bool = False
    # True when the engine confidently reports no text (e.g. vision "NO_TEXT").
    # False for soft misses (local OCR empty) — caller should try the next engine.
    authoritative_empty: bool = False
    engine: str = ""


def _download_image(url: str, timeout: int = 10) -> tuple[bytes | None, bool]:
    """Return (bytes, download_error). download_error True on HTTP/network failure."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content, False
    except requests.RequestException:
        return None, True


def _ocr_models() -> list[str]:
    """Ordered OpenRouter vision model ids (primary + configured fallbacks)."""
    primary = (getattr(config, "OCR_MODEL", None) or "").strip()
    raw = getattr(config, "OCR_FALLBACK_MODELS", "") or ""
    fallbacks = [m.strip() for m in str(raw).split(",") if m.strip()]
    models: list[str] = []
    for mid in ([primary] if primary else []) + fallbacks:
        if mid not in models:
            models.append(mid)
    return models


def _tesseract_available() -> bool:
    if not getattr(config, "OCR_USE_TESSERACT", True):
        return False
    try:
        import pytesseract  # noqa: F401
        from PIL import Image  # noqa: F401
    except ImportError:
        return False
    try:
        import pytesseract as _pt

        _pt.get_tesseract_version()
        return True
    except Exception:
        return False


def _ocr_tesseract(image_bytes: bytes) -> _EngineResult:
    """Local/offline OCR. Empty result is a soft miss (try vision next)."""
    name = "tesseract"
    try:
        import pytesseract
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes))
        lang = getattr(config, "OCR_TESSERACT_LANG", None) or "eng+kor"
        text = pytesseract.image_to_string(img, lang=lang).strip()
        if not text:
            # Soft miss — stylized TikTok overlays often defeat Tesseract.
            return _EngineResult(engine=name)
        return _EngineResult(text=text, engine=name)
    except Exception:
        log.debug("Tesseract OCR failed", exc_info=True)
        return _EngineResult(engine_error=True, engine=name)


def _ocr_openrouter(image_bytes: bytes, model: str) -> _EngineResult:
    """OpenRouter chat-completions vision OCR for one model id."""
    name = f"openrouter:{model}"
    api_key = getattr(config, "OPENROUTER_API_KEY", "") or ""
    if not api_key:
        log.debug("Skipping %s — OPENROUTER_API_KEY not set", name)
        return _EngineResult(engine_error=True, engine=name)

    b64 = base64.b64encode(image_bytes).decode("utf-8")
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                    },
                    {"type": "text", "text": _OCR_PROMPT},
                ],
            }
        ],
        "temperature": 0.1,
        "max_tokens": 500,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            config.OPENROUTER_BASE_URL,
            json=payload,
            headers=headers,
            timeout=30,
        )
        if resp.status_code in (401, 403, 404, 408, 429) or resp.status_code >= 500:
            log.warning(
                "OCR engine %s HTTP %s — trying next engine",
                name,
                resp.status_code,
            )
            return _EngineResult(engine_error=True, engine=name)
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"].strip()
        if text == "NO_TEXT" or not text:
            # Authoritative empty — not an HTTP failure.
            return _EngineResult(authoritative_empty=True, engine=name)
        return _EngineResult(text=text, engine=name)
    except (requests.RequestException, KeyError, IndexError, TypeError, ValueError):
        log.warning("OCR engine %s request failed — trying next", name, exc_info=True)
        return _EngineResult(engine_error=True, engine=name)


def _iter_engines():
    """Yield callables (image_bytes) -> _EngineResult in preference order."""
    if _tesseract_available():
        yield _ocr_tesseract
    for model in _ocr_models():

        def _make(m: str):
            return lambda image_bytes, _m=m: _ocr_openrouter(image_bytes, _m)

        yield _make(model)


def _ocr_image(image_bytes: bytes) -> tuple[str | None, bool, str | None]:
    """Run the OCR fallback chain on one image.

    Returns (text, engine_error, engine_name).
    engine_error True only when every configured engine was down (404/timeout/
    auth/etc). ``NO_TEXT`` is not an engine_error.
    """
    engines = list(_iter_engines())
    if not engines:
        log.error("No OCR engines configured (no Tesseract, no OpenRouter models)")
        return None, True, None

    for run in engines:
        result = run(image_bytes)
        if result.engine_error:
            continue
        if result.text:
            log.info("OCR succeeded via %s", result.engine)
            return result.text, False, result.engine
        if result.authoritative_empty:
            log.info("OCR %s reported NO_TEXT", result.engine)
            return None, False, result.engine
        # Soft empty (local) — fall through to the next engine.

    return None, True, None


def _process_one(post_id: int, image_urls: list[str]) -> _OCRAttempt:
    """Download + OCR one or more images for a post."""
    texts: list[str] = []
    engines_used: list[str] = []
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
        text, engine_err, engine = _ocr_image(image_bytes)
        if engine_err:
            any_http = True
            continue
        if engine:
            engines_used.append(engine)
        if text:
            texts.append(text)

    if not attempted:
        return _OCRAttempt(post_id, download_error=True)

    combined = "\n".join(texts) if texts else None
    engine_label = ",".join(dict.fromkeys(engines_used)) if engines_used else None
    return _OCRAttempt(
        post_id,
        combined,
        http_error=any_http and not combined,
        download_error=any_download and not combined and not any_http,
        engine=engine_label,
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
) -> int:
    """OCR cover/slideshow images for posts not yet OCR'd.

    Loops until no pending posts remain. Appends on-screen text to captions.
    Never aborts the city run when OCR engines are down — posts are marked
    ``failed`` and discover continues.
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
                        (result.post_id,),
                    )
                    log.warning(
                        "OCR all engines failed for post %d — marked failed, continuing",
                        result.post_id,
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
                    log.info(
                        "OCR enriched post %d via %s",
                        result.post_id,
                        result.engine or "unknown",
                    )
                else:
                    # No text or download-only miss — mark done so we don't spin forever
                    status = "empty" if not result.download_error else "failed"
                    if result.download_error:
                        total_http_errors += 1
                    conn.execute(
                        "UPDATE raw_posts SET ocr_status = ? WHERE id = ?",
                        (status, result.post_id),
                    )
                    if status == "empty":
                        log.info(
                            "OCR post %d: no on-screen text (%s)",
                            result.post_id,
                            result.engine or "unknown",
                        )

        conn.commit()

    if total_attempted == 0:
        log.info("No posts with cover/slideshow images to OCR for %s", city_name)
    else:
        if total_http_errors:
            log.warning(
                "Visual OCR for %s finished with gaps: %d enriched, %d attempted, "
                "%d engine failures (city run continues)",
                city_name,
                total_enriched,
                total_attempted,
                total_http_errors,
            )
        else:
            log.info(
                "Visual OCR complete for %s: %d enriched, %d attempted, %d errors",
                city_name,
                total_enriched,
                total_attempted,
                total_http_errors,
            )
    return total_enriched
